/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.task.backpressure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.execute.BoltExecutors;
import com.alibaba.jstorm.task.master.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.TopoMasterCtrlEvent.EventType;
import com.alibaba.jstorm.utils.IntervalCheck;

/**
 * Responsible for checking if back pressure shall be triggered. 
 * When heavy load (the size of the queue monitored reaches high water mark), start back pressure, 
 * and when load goes down, stop back pressure.
 *         
 * @author Basti Liu
 */
public class BackpressureTrigger extends Backpressure {
    private static final Logger LOG = LoggerFactory.getLogger(BackpressureTrigger.class);

    private Task task;
    private int taskId;

    // Queue which is going to be monitored
    private DisruptorQueue exeQueue;
    private DisruptorQueue recvQueue;

    private BoltExecutors boltExecutor;

    private volatile boolean isUnderBackpressure = false;

    private IntervalCheck intervalCheck;

    OutputCollector output;

    private List<EventType> samplingSet;
    private double triggerSampleRate;

    public BackpressureTrigger(Task task, BoltExecutors boltExecutor, Map stormConf, OutputCollector output) {
        super(stormConf);

        this.task = task;
        this.taskId = task.getTaskId();

        int sampleNum = ConfigExtension.getBackpressureTriggerSampleNumber(stormConf);
        int smapleInterval = sampleNum * (ConfigExtension.getBackpressureCheckIntervl(stormConf));
        this.intervalCheck = new IntervalCheck();
        this.intervalCheck.setIntervalMs(smapleInterval);
        this.intervalCheck.start();

        this.samplingSet = new ArrayList<EventType>();
        this.triggerSampleRate = ConfigExtension.getBackpressureTriggerSampleRate(stormConf);

        this.output = output;

        this.boltExecutor = boltExecutor;

        try {
            StormClusterState zkCluster = task.getZkCluster();
            Map<String, SourceBackpressureInfo> backpressureInfo = zkCluster.get_backpressure_info(task.getTopologyId());
            if (backpressureInfo != null) {
                for (Entry<String, SourceBackpressureInfo> entry : backpressureInfo.entrySet()) {
                    SourceBackpressureInfo info = entry.getValue();
                    Map<String, TargetBackpressureInfo> targetInfoMap = info.getTargetTasks();
                    if (targetInfoMap != null) {
                        TargetBackpressureInfo targetInfo = targetInfoMap.get(task.getComponentId());
                        if (targetInfo != null && targetInfo.getTasks().contains(taskId)) {
                            isBackpressureEnable = true;
                            LOG.info("Retrieved backpressure info for task-" + taskId);
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.info("Failed to get backpressure info from zk", e);
        }
        LOG.info("Finished BackpressureTrigger init, highWaterMark=" + highWaterMark + ", lowWaterMark=" + lowWaterMark + ", sendInterval="
                + intervalCheck.getInterval());
    }

    public void checkAndTrigger() {
        if (isBackpressureEnable == false) {
            return;
        }

        if (exeQueue == null || recvQueue == null) {
            exeQueue = task.getExecuteQueue();
            recvQueue = task.getDeserializeQueue();
            
            if (exeQueue == null) {
                LOG.info("Init of excutor-task-" + taskId + " has not been finished!");
                return;
            }
            if (recvQueue == null) {
                LOG.info("Init of receiver-task-" + taskId + " has not been finished!");
                return;
            }
        }

        LOG.debug("Backpressure Check: exeQueue load=" + (exeQueue.pctFull() * 100) + ", recvQueue load=" + (recvQueue.pctFull() * 100));
        if (exeQueue.pctFull() > highWaterMark) {
            samplingSet.add(EventType.startBackpressure);
        } else if (exeQueue.pctFull() <= lowWaterMark) {
            samplingSet.add(EventType.stopBackpressure);
        } else {
            samplingSet.add(EventType.defaultType);
        }

        if (intervalCheck.check()) {
            int startCount = 0, stopCount = 0;

            for (EventType eventType : samplingSet) {
                if (eventType.equals(EventType.startBackpressure)) {
                    startCount++;
                } else if (eventType.equals(EventType.stopBackpressure)) {
                    stopCount++;
                }
            }

            if (startCount > stopCount) {
                if (sampleRateCheck(startCount)) {
                    startBackpressure();
                    isUnderBackpressure = true;
                }
            } else {
                if (sampleRateCheck(stopCount) && isUnderBackpressure == true) {
                    stopBackpressure();
                }
            }

            samplingSet.clear();
        }
    }

    private boolean sampleRateCheck(double count) {
        double sampleRate = count / samplingSet.size();
        if (sampleRate > triggerSampleRate)
            return true;
        else
            return false;
    }

    public void handle(Tuple input) {
        try {
            TopoMasterCtrlEvent event = (TopoMasterCtrlEvent) input.getValueByField("ctrlEvent");
            EventType type = event.getEventType();
            if (type.equals(EventType.stopBackpressure)) {
                isUnderBackpressure = false;
                LOG.info("Received stop backpressure event for task-" + task.getTaskId());
            } else if (type.equals(EventType.updateBackpressureConfig)) {
                Map stormConf = (Map) event.getEventValue().get(0);
                updateConfig(stormConf);

                if (isBackpressureEnable == false) {
                    LOG.info("Disable backpressure in trigger.");
                    isUnderBackpressure = false;
                    samplingSet.clear();
                } else {
                    LOG.info("Enable backpressure in trigger.");
                }
            } else {
                LOG.info("Received unexpected event, " + type.toString());
            }
        } catch (Exception e) {
            LOG.error("Failed to handle event", e);
        }
    }

    private void startBackpressure() {
        List<Object> value = new ArrayList<Object>();
        Double flowCtrlTime = Double.valueOf(boltExecutor.getExecuteTime() / 1000);
        value.add(flowCtrlTime.intValue());
        TopoMasterCtrlEvent startBp = new TopoMasterCtrlEvent(EventType.startBackpressure, value);
        output.emit(Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, new Values(startBp));
        LOG.debug("Send start backpressure request for task-{}, flowCtrlTime={}", taskId, flowCtrlTime.intValue());
    }

    private void stopBackpressure() {
        TopoMasterCtrlEvent stopBp = new TopoMasterCtrlEvent(EventType.stopBackpressure, null);
        output.emit(Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, new Values(stopBp));
        LOG.debug("Send stop backpressure request for task-{}", taskId);
    }
}