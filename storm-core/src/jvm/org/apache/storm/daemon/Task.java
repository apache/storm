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
package org.apache.storm.daemon;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.Thrift;
import org.apache.storm.daemon.metrics.BuiltinMetrics;
import org.apache.storm.daemon.metrics.BuiltinMetricsUtil;
import org.apache.storm.executor.Executor;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentObject;
import org.apache.storm.generated.JavaObject;
import org.apache.storm.generated.ShellComponent;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.LoadAwareCustomStreamGrouping;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.spout.ShellSpout;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    private Executor executor;
    private Map workerData;
    private TopologyContext systemTopologyContext;
    private TopologyContext userTopologyContext;
    private WorkerTopologyContext workerTopologyContext;
    private LoadMapping loadMapping;
    private Integer taskId;
    private String componentId;
    private Object taskObject; // Spout/Bolt object
    private Map stormConf;
    private Callable<Boolean> emitSampler;
    private CommonStats executorStats;
    private Map<String, Map<String, LoadAwareCustomStreamGrouping>> streamComponentToGrouper;
    private BuiltinMetrics builtInMetrics;
    private boolean debug;

    public Task(Executor executor, Integer taskId) throws IOException {
        this.taskId = taskId;
        this.executor = executor;
        this.workerData = executor.getWorkerData();
        this.stormConf = executor.getStormConf();
        this.componentId = executor.getComponentId();
        this.streamComponentToGrouper = executor.getStreamToComponentToGrouper();
        this.executorStats = executor.getStats();
        this.builtInMetrics = BuiltinMetricsUtil.mkData(executor.getType(), this.executorStats);
        this.workerTopologyContext = executor.getWorkerTopologyContext();
        this.emitSampler = ConfigUtils.mkStatsSampler(stormConf);
        this.loadMapping = (LoadMapping) workerData.get(Constants.LOAD_MAPPING);
        this.systemTopologyContext = mkTopologyContext((StormTopology) workerData.get(Constants.SYSTEM_TOPOLOGY));
        this.userTopologyContext = mkTopologyContext((StormTopology) workerData.get(Constants.TOPOLOGY));
        this.taskObject = mkTaskObject();
        this.debug = stormConf.containsKey(Config.TOPOLOGY_DEBUG) && (Boolean) stormConf.get(Config.TOPOLOGY_DEBUG);
        this.addTaskHooks();
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
        new EmitInfo(values, stream, taskId, Collections.singletonList(outTaskId)).applyOn(userTopologyContext);
        try {
            if (emitSampler.call()) {
                executorStats.emittedTuple(stream);
                if (null != outTaskId) {
                    executorStats.transferredTuples(stream, 1);
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
            LOG.info("Emitting: {} {} {}", componentId, stream, values);
        }

        List<Integer> outTasks = new ArrayList<>();
        if (!streamComponentToGrouper.containsKey(stream)) {
            throw new IllegalArgumentException("Unknown stream ID: " + stream);
        }
        if (null != streamComponentToGrouper.get(stream)) {
            // null value for __system
            for (LoadAwareCustomStreamGrouping grouper : streamComponentToGrouper.get(stream).values()) {
                if (grouper == GrouperFactory.DIRECT) {
                    throw new IllegalArgumentException("Cannot do regular emit to direct stream");
                }
                List<Integer> compTasks = grouper.chooseTasks(taskId, values, loadMapping);
                outTasks.addAll(compTasks);
            }
        }
        new EmitInfo(values, stream, taskId, outTasks).applyOn(userTopologyContext);
        try {
            if (emitSampler.call()) {
                executorStats.emittedTuple(stream);
                executorStats.transferredTuples(stream, outTasks.size());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return outTasks;
    }

    public Tuple getTuple(String stream, List values) {
        return new TupleImpl(systemTopologyContext, values, systemTopologyContext.getThisTaskId(), stream);
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

    public BuiltinMetrics getBuiltInMetrics() {
        return builtInMetrics;
    }

    private TopologyContext mkTopologyContext(StormTopology topology) throws IOException {
        Map conf = (Map) workerData.get(Constants.CONF);
        return new TopologyContext(
                topology,
                (Map) workerData.get(Constants.STORM_CONF),
                (Map<Integer, String>) workerData.get(Constants.TASK_TO_COMPONENT),
                (Map<String, List<Integer>>) workerData.get(Constants.COMPONENT_TO_SORTED_TASKS),
                (Map<String, Map<String, Fields>>) workerData.get(Constants.COMPONENT_TO_STREAM_TO_FIELDS),
                (String) workerData.get(Constants.STORM_ID),
                ConfigUtils.supervisorStormResourcesPath(
                        ConfigUtils.supervisorStormDistRoot(conf, (String) workerData.get(Constants.STORM_ID))),
                ConfigUtils.workerPidsRoot(conf, (String) workerData.get(Constants.WORKER_ID)),
                taskId,
                (Integer) workerData.get(Constants.PORT),
                (List<Integer>) workerData.get(Constants.TASK_IDS),
                (Map<String, Object>) workerData.get(Constants.DEFAULT_SHARED_RESOURCES),
                (Map<String, Object>) workerData.get(Constants.USER_SHARED_RESOURCES),
                executor.getSharedExecutorData(),
                executor.getIntervalToTaskToMetricToRegistry(),
                executor.getOpenOrPrepareWasCalled());
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
        List<String> hooksClassList = (List<String>) stormConf.get(Config.TOPOLOGY_AUTO_TASK_HOOKS);
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

}
