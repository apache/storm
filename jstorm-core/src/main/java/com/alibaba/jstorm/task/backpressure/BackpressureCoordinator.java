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

import backtype.storm.generated.*;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.*;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.master.TopoMasterCtrlEvent;
import com.alibaba.jstorm.task.master.TopoMasterCtrlEvent.EventType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

/**
 * Coordinator is responsible for the request from trigger and controller.
 * - Event from trigger:
 *   Find relative controllers (source spouts), and decide if it is required to send out the request.
 * - Event from controller: 
 *   If backpressure stop event, send stop request to all target triggers.
 * 
 * @author Basti Li
 */
public class BackpressureCoordinator extends Backpressure {
    private static final Logger LOG = LoggerFactory.getLogger(BackpressureCoordinator.class);

    private static final int adjustedTime = 5;

    private TopologyContext context;
    private StormTopology topology;
    private OutputCollector output;

    private int topologyMasterId;
    private Map<Integer, String> taskIdToComponentId;
    private Map<String, SpoutSpec> spouts;
    private Map<String, Bolt> bolts;

    // Map<source componentId, Map<ComponentId, backpressure info>>
    private Map<String, SourceBackpressureInfo> SourceTobackpressureInfo;

    private Integer period;

    private StormClusterState zkCluster;
    private static final String BACKPRESSURE_TAG = "Backpressure has been ";


    public BackpressureCoordinator(OutputCollector output, TopologyContext topologyContext, Integer taskId) {
        super(topologyContext.getStormConf());
        this.context = topologyContext;
        this.topology = topologyContext.getRawTopology();
        this.spouts = new HashMap<String, SpoutSpec>(); 
        if (this.topology.get_spouts() != null) {
            this.spouts.putAll(this.topology.get_spouts());
        }
        this.bolts = new HashMap<String, Bolt>(); 
        if (this.topology.get_bolts() != null) {
            this.bolts.putAll(this.topology.get_bolts());
        }
        this.taskIdToComponentId = topologyContext.getTaskToComponent();
        this.topologyMasterId = taskId;

        this.output = output;

        int checkInterval = ConfigExtension.getBackpressureCheckIntervl(context.getStormConf());
        int sampleNum = ConfigExtension.getBackpressureTriggerSampleNumber(context.getStormConf());
        this.period = checkInterval * sampleNum;

        this.zkCluster = topologyContext.getZkCluster();
        try {
            this.SourceTobackpressureInfo = zkCluster.get_backpressure_info(context.getTopologyId());
            if (this.SourceTobackpressureInfo == null) {
                this.SourceTobackpressureInfo = new HashMap<String, SourceBackpressureInfo>();
            } else {
                LOG.info("Successfully retrieve existing SourceTobackpressureInfo from zk: " + SourceTobackpressureInfo);
            }
        } catch (Exception e) {
            LOG.warn("Failed to get SourceTobackpressureInfo from zk", e);
            this.SourceTobackpressureInfo = new HashMap<String, SourceBackpressureInfo>();
        }
    }

    private Set<String> getInputSpoutsForBolt(StormTopology topology, String boltComponentId, Set<String> componentsTraversed) {
        Set<String> ret = new TreeSet<String>();

        if (componentsTraversed == null) {
            componentsTraversed = new HashSet<String>();
        }

        Bolt bolt = bolts.get(boltComponentId);
        if (bolt == null) {
            return ret;
        }

        ComponentCommon common = bolt.get_common();
        Set<GlobalStreamId> inputstreams = common.get_inputs().keySet();
        Set<String> inputComponents = new TreeSet<String>();
        for (GlobalStreamId streamId : inputstreams) {
            inputComponents.add(streamId.get_componentId());
        }

        Set<String> spoutComponentIds = new HashSet<String>(spouts.keySet());
        Set<String> boltComponentIds = new HashSet<String>(bolts.keySet());
        for (String inputComponent : inputComponents) {
            // Skip the components which has been traversed before, to avoid dead loop when there are loop bolts in topology
            if (componentsTraversed.contains(inputComponent)) {
                continue;
            } else {
                componentsTraversed.add(inputComponent);
            }

            if (spoutComponentIds.contains(inputComponent)) {
                ret.add(inputComponent);
            } else if (boltComponentIds.contains(inputComponent)) {
                Set<String> inputs = getInputSpoutsForBolt(topology, inputComponent, componentsTraversed);
                ret.addAll(inputs);
            }
        }

        return ret;
    }

    public void process(Tuple input) {
        if (isBackpressureEnable == false) {
            return;
        }

        int sourceTask = input.getSourceTask();
        String componentId = taskIdToComponentId.get(sourceTask);
        if (componentId == null) {
            LOG.warn("Receive tuple from unknown task-" + sourceTask);
            return;
        }

        if (spouts.keySet().contains(componentId)) {
            if (SourceTobackpressureInfo.get(componentId) != null) {
                handleEventFromSpout(sourceTask, input);
            }
        } else if (bolts.keySet().contains(componentId)) {
            handleEventFromBolt(sourceTask, input);
        }
    }

    public void updateBackpressureConfig(Map conf) {
        updateConfig(conf);

        if (isBackpressureEnable == false) {
            LOG.info("Disable backpressure in coordinator.");
            SourceTobackpressureInfo.clear();
        } else {
            LOG.info("Enable backpressure in coordinator.");
        }

        TopoMasterCtrlEvent updateBpConfig = new TopoMasterCtrlEvent(EventType.updateBackpressureConfig, new ArrayList<Object>());
        updateBpConfig.addEventValue(conf);
        Values values = new Values(updateBpConfig);
        Set<Integer> targetTasks = new TreeSet<Integer>(taskIdToComponentId.keySet());
        targetTasks.remove(topologyMasterId);
        targetTasks.removeAll(context.getComponentTasks(Acker.ACKER_COMPONENT_ID));
        sendBackpressureMessage(targetTasks, values, EventType.updateBackpressureConfig);

        reportBackpressureStatus();
    }

    private boolean checkSpoutsUnderBackpressure(Set<String> spouts) {
        boolean ret = false;

        if (spouts != null) {
            for (String spout : spouts) {
                SourceBackpressureInfo backpressureInfo = SourceTobackpressureInfo.get(spout);
                if (backpressureInfo != null && backpressureInfo.getTasks().size() > 0) {
                    ret = true;
                    break;
                }
            }
        }

        return ret;
    }

    private TargetBackpressureInfo getBackpressureInfoBySourceSpout(String sourceSpout, String targetComponentId, boolean created) {
        TargetBackpressureInfo ret = null;

        SourceBackpressureInfo info = SourceTobackpressureInfo.get(sourceSpout);
        if (info == null) {
            if (created) {
                info = new SourceBackpressureInfo();
                SourceTobackpressureInfo.put(sourceSpout, info);
            }
        } else {
            ret = info.getTargetTasks().get(targetComponentId);
        }
    
        if (ret == null && created) {
            ret = new TargetBackpressureInfo();
            info.getTargetTasks().put(targetComponentId, ret);
        }
        return ret;
    }

    private boolean checkIntervalExpired(long time) {
        boolean ret = false;
        if (time != 0) {
            if (System.currentTimeMillis() - time > period) {
                ret = true;
            }
        }
        return ret;
    }

    private void sendBackpressureMessage(Set<Integer> targetTasks, Values value, EventType backpressureType) {
        for (Integer taskId : targetTasks) {
            output.emitDirect(taskId, Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID, value);
            LOG.debug("Send " + backpressureType.toString() + " request to taskId-" + taskId);
        }
    }

    private void handleEventFromSpout(int sourceTask, Tuple input) {
        TopoMasterCtrlEvent ctrlEvent = (TopoMasterCtrlEvent) input.getValueByField("ctrlEvent");
        EventType type = ctrlEvent.getEventType();

        boolean update = false;
        if (type.equals(EventType.stopBackpressure)) {
            String spoutComponentId = taskIdToComponentId.get(sourceTask);
            SourceBackpressureInfo info = SourceTobackpressureInfo.remove(spoutComponentId);
            if (info != null) {
                info.getTasks().remove(sourceTask);
                if (info.getTasks().size() == 0) {  
                    for (Entry<String, TargetBackpressureInfo> entry : info.getTargetTasks().entrySet()) {
                        String componentId = entry.getKey();

                        // Make sure if all source spouts for this bolt are NOT under backpressure mode.
                        Set<String> sourceSpouts = getInputSpoutsForBolt(topology, componentId, null);
                        if (checkSpoutsUnderBackpressure(sourceSpouts) == false) {
                            Set<Integer> tasks = new TreeSet<Integer>();
                            tasks.addAll(context.getComponentTasks(componentId));
                            sendBackpressureMessage(tasks, new Values(ctrlEvent), type);
                        }
                    }
                }
                update = true;
            } else {
                LOG.error("Received event from non-recorded spout-" + sourceTask);
            }

        } else {
            LOG.warn("Received unexpected event, " + type.toString());
        }

        // If task set under backpressure has been changed, report the latest status
        if (update) {
            reportBackpressureStatus();
        }
    }

    private void handleEventFromBolt(int sourceTask, Tuple input) {
        String componentId = taskIdToComponentId.get(sourceTask);
        Set<String> inputSpouts = getInputSpoutsForBolt(topology, componentId, null);

        TopoMasterCtrlEvent ctrlEvent = (TopoMasterCtrlEvent) input.getValueByField("ctrlEvent");
        EventType type = ctrlEvent.getEventType();
        Set<Integer> notifyList = new TreeSet<Integer>();
        Values values = null;
        TargetBackpressureInfo info = null;
        boolean update = false;
        if (type.equals(EventType.startBackpressure)) {
            int flowCtrlTime = (Integer) ctrlEvent.getEventValue().get(0);
            for (String spout : inputSpouts) {
                info = getBackpressureInfoBySourceSpout(spout, componentId, true);
                SourceBackpressureInfo sourceInfo = SourceTobackpressureInfo.get(spout);
                update = info.getTasks().add(sourceTask);
                boolean add = false;
                if (System.currentTimeMillis() - sourceInfo.getLastestTimeStamp() > period) { 
                    add = true;
                } else {
                    EventType lastestBpEvent = sourceInfo.getLastestBackpressureEvent();
                    if (lastestBpEvent != null && lastestBpEvent.equals(EventType.startBackpressure) == false) {
                        add = true;
                    }

                    int maxFlowCtrlTime = sourceInfo.getMaxFlowCtrlTime();
                    if ((flowCtrlTime - maxFlowCtrlTime > adjustedTime || maxFlowCtrlTime == -1) &&
                            flowCtrlTime >= 0) {
                        add = true;
                    }
                }
                info.setFlowCtrlTime(flowCtrlTime);
                info.setBackpressureStatus(type);

                if (add) {
                    info.setTimeStamp(System.currentTimeMillis());
                    // Only when the number of bolt tasks sending request is more than a configured number, coordinator will 
                    // send out backpressure request to controller. It is used to avoid the problem that very few tasks might
                    // cause the over control.
                    double taskBpRatio = Double.valueOf(info.getTasks().size()) / Double.valueOf(context.getComponentTasks(componentId).size()) ;
                    if (taskBpRatio >= triggerBpRatio) {
                        Set<Integer> spoutTasks = new TreeSet<Integer>(context.getComponentTasks(spout));
                        if (spoutTasks != null) {
                            SourceTobackpressureInfo.get(spout).getTasks().addAll(spoutTasks);
                            notifyList.addAll(spoutTasks);
                        }
                    } else {
                        update = false;
                    }
                } else {
                    update = false;
                }
            }

            List<Object> value = new ArrayList<Object>();
            value.add(info.getFlowCtrlTime());
            TopoMasterCtrlEvent startBp = new TopoMasterCtrlEvent(EventType.startBackpressure, value);
            values = new Values(startBp);
        } else if (type.equals(EventType.stopBackpressure)) {
            for (String spout : inputSpouts) {
                info = getBackpressureInfoBySourceSpout(spout, componentId, false);
                SourceBackpressureInfo sourceInfo = SourceTobackpressureInfo.get(spout);
                if (info != null) {
                    Set<Integer> tasks = info.getTasks();
                    if (tasks != null) {
                        if(tasks.remove(sourceTask)) {
                            update = true;
                        }
                    }
                }

                if (sourceInfo != null && checkIntervalExpired(sourceInfo.getLastestTimeStamp())) {
                    info.setTimeStamp(System.currentTimeMillis());
                    Set<Integer> spoutTasks = new TreeSet<Integer>(context.getComponentTasks(spout));
                    if (spoutTasks != null) {
                        notifyList.addAll(spoutTasks);
                    }
                    info.setBackpressureStatus(type);
                }
            }

            // Check if all source spouts are Not under backpressure. If so, notify the bolt.
            if (checkSpoutsUnderBackpressure(inputSpouts) == false) {
                notifyList.add(sourceTask);
            }

            TopoMasterCtrlEvent stoptBp = new TopoMasterCtrlEvent(EventType.stopBackpressure, null);
            values = new Values(stoptBp);
        } else {
            LOG.warn("Received unknown event " + type.toString());
        }

        sendBackpressureMessage(notifyList, values, type);

        // If task set under backpressure has been changed, report the latest status
        if (update) {
            LOG.info("inputspouts=" + inputSpouts + " for " + componentId + "-" + sourceTask + ", eventType=" + type.toString());
            reportBackpressureStatus();
        }
    }

    private Set<Integer> getTasksUnderBackpressure() {
        Set<Integer> ret = new HashSet<Integer>();

        for (Entry<String, SourceBackpressureInfo> entry : SourceTobackpressureInfo.entrySet()) {
            SourceBackpressureInfo sourceInfo = entry.getValue();
            if (sourceInfo.getTasks().size() > 0) {
                ret.addAll(sourceInfo.getTasks());

                for (Entry<String, TargetBackpressureInfo> targetEntry: sourceInfo.getTargetTasks().entrySet()) {
                    ret.addAll(targetEntry.getValue().getTasks());
                }
                
            }
        }

        return ret;
    }

    private void reportBackpressureStatus() {
        try {
            StringBuilder stringBuilder = new StringBuilder();
            Set<Integer> underTasks = getTasksUnderBackpressure();
            stringBuilder.append(BACKPRESSURE_TAG);
            if (underTasks.isEmpty()){
                stringBuilder.append("closed ");
            }else {
                stringBuilder.append("opened: ");
                stringBuilder.append(underTasks);
            }
            zkCluster.report_task_error(context.getTopologyId(), context.getThisTaskId(), stringBuilder.toString(), BACKPRESSURE_TAG);
            zkCluster.set_backpressure_info(context.getTopologyId(), SourceTobackpressureInfo);
            LOG.info(stringBuilder.toString());
        } catch (Exception e) {
            LOG.error("can't update backpressure state ", e);
        }
    }
}
