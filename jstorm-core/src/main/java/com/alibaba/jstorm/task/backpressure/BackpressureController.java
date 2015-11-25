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

import java.util.List;
import java.util.Map;

import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.task.TaskTransfer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.task.master.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.TopoMasterCtrlEvent.EventType;

/**
 * Flow Control
 * 
 * @author Basti Liu
 */
public class BackpressureController extends Backpressure {
    private static Logger LOG = LoggerFactory.getLogger(BackpressureController.class);

    private int taskId;
    private DisruptorQueue queueControlled;
    private int totalQueueSize;
    private int queueSizeReduced;

    private boolean isBackpressureMode = false;

    private SpoutOutputCollector outputCollector;

    private long maxBound, minBound;

    public BackpressureController(Map conf, int taskId, DisruptorQueue queue, int queueSize) {
        super(conf);
        this.queueControlled = queue;
        this.totalQueueSize = queueSize;
        this.queueSizeReduced = queueSize;
        this.taskId = taskId;
        this.maxBound = 0l;
        this.minBound = 0l;
    }

    public void setOutputCollector(SpoutOutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void control(TopoMasterCtrlEvent ctrlEvent) {
        if (isBackpressureEnable == false) {
            return;
        }

        EventType eventType = ctrlEvent.getEventType();
        LOG.debug("Received control event, " + eventType.toString());
        if (eventType.equals(EventType.startBackpressure)) {
            List<Object> value = ctrlEvent.getEventValue();
            int flowCtrlTime = value.get(0) != null ? (Integer) value.get(0) : 0;
            start(flowCtrlTime);
        } else if (eventType.equals(EventType.stopBackpressure)) {
            stop();
        } else if (eventType.equals(EventType.updateBackpressureConfig)) {
            List<Object> value = ctrlEvent.getEventValue();
            if (value != null) {
                Map stormConf = (Map) value.get(0);
                updateConfig(stormConf);

                if (isBackpressureEnable == false) {
                    LOG.info("Disable backpressure in controller.");
                    resetBackpressureInfo();
                } else {
                    LOG.info("Enable backpressure in controller");
                }
            }
        }
    }

    public void flowControl() {
        if (isBackpressureEnable == false) {
            return;
        }

        try {
            Thread.sleep(sleepTime);
            while (isQueueCapacityAvailable() == false) {
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            LOG.error("Sleep was interrupted!");
        }
    }

    private void resetBackpressureInfo() {
        sleepTime = 0l;
        maxBound = 0l;
        minBound = 0l;
        queueSizeReduced = totalQueueSize;
        isBackpressureMode = false;
    }

    private void start(int flowCtrlTime) {
        if (flowCtrlTime > 0) {
            if (maxBound < flowCtrlTime) {
                sleepTime = flowCtrlTime;
            } else if (maxBound == flowCtrlTime) {
                if (sleepTime >= maxBound) {
                    sleepTime++;
                } else {
                    sleepTime = JStormUtils.halfValueOfSum(flowCtrlTime, sleepTime, true);
                } 
            } else {
                if (maxBound <= sleepTime) {
                    sleepTime++;
                } else {
                    if (sleepTime >= flowCtrlTime) {
                        sleepTime = JStormUtils.halfValueOfSum(maxBound, sleepTime, true);
                    } else {
                        sleepTime = JStormUtils.halfValueOfSum(flowCtrlTime, sleepTime, true);
                    }
                }
            }
        } else {
            sleepTime++;
        }
        if (sleepTime > maxBound) {
            maxBound = sleepTime;
        }

        int size = totalQueueSize / 100;
        queueSizeReduced = size > 10 ? size : 10;
        isBackpressureMode = true;

        LOG.info("Start backpressure at spout-{}, sleepTime={}, queueSizeReduced={}, flowCtrlTime={}", taskId, sleepTime, queueSizeReduced, flowCtrlTime);
    }

    private void stop() {
        if (sleepTime == minBound) {
            minBound = 0;
        }
        sleepTime = JStormUtils.halfValueOfSum(minBound, sleepTime, false);

        if (sleepTime == 0) {
            resetBackpressureInfo();

            TopoMasterCtrlEvent stopBp = new TopoMasterCtrlEvent(EventType.stopBackpressure, null);
            outputCollector.emit(Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, new Values(stopBp));
        } else {
            minBound = sleepTime;
        }

        LOG.info("Stop backpressure at spout-{}, sleepTime={}, queueSizeReduced={}, flowCtrlTime={}", taskId, sleepTime, queueSizeReduced);
    }

    public boolean isBackpressureMode() {
        return isBackpressureMode & isBackpressureEnable;
    }

    public boolean isQueueCapacityAvailable() {
        return (queueControlled.population() < queueSizeReduced);
    }
}
