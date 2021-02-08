/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.daemon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.function.BooleanSupplier;
import org.apache.storm.Config;
import org.apache.storm.Thrift;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.executor.Executor;
import org.apache.storm.executor.ExecutorTransfer;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentObject;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.generated.JavaObject;
import org.apache.storm.generated.ShellComponent;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.LoadAwareCustomStreamGrouping;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.metrics2.TaskMetrics;
import org.apache.storm.spout.ShellSpout;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);
    private final TaskMetrics taskMetrics;
    private final Executor executor;
    private final WorkerState workerData;
    private final TopologyContext systemTopologyContext;
    private final TopologyContext userTopologyContext;
    private final WorkerTopologyContext workerTopologyContext;
    private final Integer taskId;
    private final String componentId;
    private final Object taskObject; // Spout/Bolt object
    private final Map<String, Object> topoConf;
    private final BooleanSupplier emitSampler;
    private final CommonStats executorStats;
    private final Map<String, Map<String, LoadAwareCustomStreamGrouping>> streamComponentToGrouper;
    private final HashMap<String, ArrayList<LoadAwareCustomStreamGrouping>> streamToGroupers;
    private final boolean debug;

    public Task(Executor executor, Integer taskId) throws IOException {
        this.taskId = taskId;
        this.executor = executor;
        this.workerData = executor.getWorkerData();
        this.topoConf = executor.getTopoConf();
        this.componentId = executor.getComponentId();
        this.streamComponentToGrouper = executor.getStreamToComponentToGrouper();
        this.streamToGroupers = getGroupersPerStream(streamComponentToGrouper);
        this.executorStats = executor.getStats();
        this.workerTopologyContext = executor.getWorkerTopologyContext();
        this.emitSampler = ConfigUtils.mkStatsSampler(topoConf);
        this.systemTopologyContext = mkTopologyContext(workerData.getSystemTopology());
        this.userTopologyContext = mkTopologyContext(workerData.getTopology());
        this.taskObject = mkTaskObject();
        this.debug = topoConf.containsKey(Config.TOPOLOGY_DEBUG) && (Boolean) topoConf.get(Config.TOPOLOGY_DEBUG);
        this.addTaskHooks();
        this.taskMetrics = new TaskMetrics(this.workerTopologyContext, this.componentId, this.taskId,
                workerData.getMetricRegistry(), topoConf);
    }

    private static HashMap<String, ArrayList<LoadAwareCustomStreamGrouping>> getGroupersPerStream(
        Map<String, Map<String, LoadAwareCustomStreamGrouping>> streamComponentToGrouper) {
        HashMap<String, ArrayList<LoadAwareCustomStreamGrouping>> result = new HashMap<>(streamComponentToGrouper.size());

        for (Entry<String, Map<String, LoadAwareCustomStreamGrouping>> entry : streamComponentToGrouper.entrySet()) {
            String stream = entry.getKey();
            Map<String, LoadAwareCustomStreamGrouping> groupers = entry.getValue();
            ArrayList<LoadAwareCustomStreamGrouping> perStreamGroupers = new ArrayList<>();
            if (groupers != null) { // null for __system bolt
                for (LoadAwareCustomStreamGrouping grouper : groupers.values()) {
                    perStreamGroupers.add(grouper);
                }
            }
            result.put(stream, perStreamGroupers);
        }
        return result;
    }

    public List<Integer> getOutgoingTasks(Integer outTaskId, String stream, List<Object> values) {
        if (debug) {
            LOG.info("Emitting direct: {}; {} {} {} ", outTaskId, componentId, stream, values);
        }
        String targetComponent = workerTopologyContext.getComponentId(outTaskId);
        Map<String, LoadAwareCustomStreamGrouping> componentGrouping = streamComponentToGrouper.get(stream);
        LoadAwareCustomStreamGrouping grouping = componentGrouping.get(targetComponent);
        if (null == grouping) {
            outTaskId = null;
        }
        if (grouping != null && grouping != GrouperFactory.DIRECT) {
            throw new IllegalArgumentException("Cannot emitDirect to a task expecting a regular grouping");
        }
        if (!userTopologyContext.getHooks().isEmpty()) {
            new EmitInfo(values, stream, taskId, Collections.singletonList(outTaskId)).applyOn(userTopologyContext);
        }

        try {
            if (emitSampler.getAsBoolean()) {
                executorStats.emittedTuple(stream);
                this.taskMetrics.emittedTuple(stream);
                if (null != outTaskId) {
                    executorStats.transferredTuples(stream, 1);
                    this.taskMetrics.transferredTuples(stream, 1);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (null != outTaskId) {
            return Collections.singletonList(outTaskId);
        }
        return new ArrayList<>(0);
    }

    public List<Integer> getOutgoingTasks(String stream, List<Object> values) {
        if (debug) {
            LOG.info("Emitting Tuple: taskId={} componentId={} stream={} values={}", taskId, componentId, stream, values);
        }

        ArrayList<Integer> outTasks = new ArrayList<>();

        ArrayList<LoadAwareCustomStreamGrouping> groupers = streamToGroupers.get(stream);
        if (null != groupers) {
            for (int i = 0; i < groupers.size(); ++i) {
                LoadAwareCustomStreamGrouping grouper = groupers.get(i);
                if (grouper == GrouperFactory.DIRECT) {
                    throw new IllegalArgumentException("Cannot do regular emit to direct stream");
                }
                List<Integer> compTasks = grouper.chooseTasks(taskId, values);
                outTasks.addAll(compTasks);
            }
        } else {
            throw new IllegalArgumentException("Unknown stream ID: " + stream);
        }

        if (!userTopologyContext.getHooks().isEmpty()) {
            new EmitInfo(values, stream, taskId, outTasks).applyOn(userTopologyContext);
        }
        try {
            if (emitSampler.getAsBoolean()) {
                executorStats.emittedTuple(stream);
                this.taskMetrics.emittedTuple(stream);
                executorStats.transferredTuples(stream, outTasks.size());
                this.taskMetrics.transferredTuples(stream, outTasks.size());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return outTasks;
    }

    public Tuple getTuple(String stream, List values) {
        return new TupleImpl(systemTopologyContext, values, executor.getComponentId(), systemTopologyContext.getThisTaskId(), stream);
    }

    public Integer getTaskId() {
        return taskId;
    }

    public String getComponentId() {
        return componentId;
    }

    public TopologyContext getUserContext() {
        return userTopologyContext;
    }

    public Object getTaskObject() {
        return taskObject;
    }

    public TaskMetrics getTaskMetrics() {
        return taskMetrics;
    }

    // Non Blocking call. If cannot emit to destination immediately, such tuples will be added to `pendingEmits` argument
    public void sendUnanchored(String stream, List<Object> values, ExecutorTransfer transfer, Queue<AddressedTuple> pendingEmits) {
        Tuple tuple = getTuple(stream, values);
        List<Integer> tasks = getOutgoingTasks(stream, values);
        for (int i = 0; i < tasks.size(); i++) {
            AddressedTuple addressedTuple = new AddressedTuple(tasks.get(i), tuple);
            transfer.tryTransfer(addressedTuple, pendingEmits);
        }
    }

    /**
     * Send sampled data to the eventlogger if the global or component level debug flag is set (via nimbus api).
     */
    public void sendToEventLogger(Executor executor, List values,
                                  String componentId, Object messageId, Random random, Queue<AddressedTuple> overflow) {
        Map<String, DebugOptions> componentDebug = executor.getStormComponentDebug().get();
        DebugOptions debugOptions = componentDebug.get(componentId);
        if (debugOptions == null) {
            debugOptions = componentDebug.get(executor.getStormId());
        }
        double spct = ((debugOptions != null) && (debugOptions.is_enable())) ? debugOptions.get_samplingpct() : 0;
        if (spct > 0 && (random.nextDouble() * 100) < spct) {
            sendUnanchored(StormCommon.EVENTLOGGER_STREAM_ID,
                           new Values(componentId, messageId, System.currentTimeMillis(), values),
                           executor.getExecutorTransfer(), overflow);
        }
    }

    private TopologyContext mkTopologyContext(StormTopology topology) throws IOException {
        Map<String, Object> conf = workerData.getConf();
        return new TopologyContext(
            topology,
            workerData.getTopologyConf(),
            workerData.getTaskToComponent(),
            workerData.getComponentToSortedTasks(),
            workerData.getComponentToStreamToFields(),
            // This is updated by the Worker and the topology has shared access to it
            workerData.getBlobToLastKnownVersion(),
            workerData.getTopologyId(),
            ConfigUtils.supervisorStormResourcesPath(
                ConfigUtils.supervisorStormDistRoot(conf, workerData.getTopologyId())),
            ConfigUtils.workerPidsRoot(conf, workerData.getWorkerId()),
            taskId,
            workerData.getPort(), workerData.getLocalTaskIds(),
            workerData.getDefaultSharedResources(),
            workerData.getUserSharedResources(),
            executor.getSharedExecutorData(),
            executor.getIntervalToTaskToMetricToRegistry(),
            executor.getOpenOrPrepareWasCalled(),
            workerData.getMetricRegistry());
    }

    private Object mkTaskObject() {
        StormTopology topology = systemTopologyContext.getRawTopology();
        Map<String, SpoutSpec> spouts = topology.get_spouts();
        Map<String, Bolt> bolts = topology.get_bolts();
        Map<String, StateSpoutSpec> stateSpouts = topology.get_state_spouts();
        Object result;
        ComponentObject componentObject;
        if (spouts.containsKey(componentId)) {
            componentObject = spouts.get(componentId).get_spout_object();
        } else if (bolts.containsKey(componentId)) {
            componentObject = bolts.get(componentId).get_bolt_object();
        } else if (stateSpouts.containsKey(componentId)) {
            componentObject = stateSpouts.get(componentId).get_state_spout_object();
        } else {
            throw new RuntimeException("Could not find " + componentId + " in " + topology);
        }
        result = Utils.getSetComponentObject(componentObject);

        if (result instanceof ShellComponent) {
            if (spouts.containsKey(componentId)) {
                result = new ShellSpout((ShellComponent) result);
            } else {
                result = new ShellBolt((ShellComponent) result);
            }
        }

        if (result instanceof JavaObject) {
            result = Thrift.instantiateJavaObject((JavaObject) result);
        }

        return result;
    }

    private void addTaskHooks() {
        List<String> hooksClassList = (List<String>) topoConf.get(Config.TOPOLOGY_AUTO_TASK_HOOKS);
        if (null != hooksClassList) {
            for (String hookClass : hooksClassList) {
                try {
                    userTopologyContext.addTaskHook(((ITaskHook) Class.forName(hookClass).newInstance()));
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new RuntimeException("Failed to add hook: " + hookClass, e);
                }
            }
        }
    }

    @Override
    public String toString() {
        return taskId.toString();
    }
}
