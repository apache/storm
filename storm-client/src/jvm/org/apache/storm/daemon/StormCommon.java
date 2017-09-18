/*
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.Thrift;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.metric.EventLoggerBolt;
import org.apache.storm.metric.MetricsConsumerBolt;
import org.apache.storm.metric.SystemBolt;
import org.apache.storm.metric.filter.FilterByMetricName;
import org.apache.storm.metric.util.DataPointExpander;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ObjectReader;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormCommon {
    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static StormCommon _instance = new StormCommon();

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     *
     * @param common a StormCommon instance
     * @return the previously set instance
     */
    public static StormCommon setInstance(StormCommon common) {
        StormCommon oldInstance = _instance;
        _instance = common;
        return oldInstance;
    }

    private static final Logger LOG = LoggerFactory.getLogger(StormCommon.class);

    public static final String SYSTEM_STREAM_ID = "__system";

    public static final String EVENTLOGGER_COMPONENT_ID = "__eventlogger";
    public static final String EVENTLOGGER_STREAM_ID = "__eventlog";

    public static final String TOPOLOGY_METRICS_CONSUMER_CLASS = "class";
    public static final String TOPOLOGY_METRICS_CONSUMER_ARGUMENT = "argument";
    public static final String TOPOLOGY_METRICS_CONSUMER_MAX_RETAIN_METRIC_TUPLES = "max.retain.metric.tuples";
    public static final String TOPOLOGY_METRICS_CONSUMER_PARALLELISM_HINT = "parallelism.hint";
    public static final String TOPOLOGY_METRICS_CONSUMER_WHITELIST = "whitelist";
    public static final String TOPOLOGY_METRICS_CONSUMER_BLACKLIST = "blacklist";
    public static final String TOPOLOGY_METRICS_CONSUMER_EXPAND_MAP_TYPE = "expandMapType";
    public static final String TOPOLOGY_METRICS_CONSUMER_METRIC_NAME_SEPARATOR = "metricNameSeparator";

    @Deprecated
    public static String getStormId(final IStormClusterState stormClusterState, final String topologyName) {
        return stormClusterState.getTopoId(topologyName).get();
    }

    public static void validateDistributedMode(Map<String, Object> conf) {
        if (ConfigUtils.isLocalMode(conf)) {
            throw new IllegalArgumentException("Cannot start server in local mode!");
        }
    }

    private static Set<String> validateIds(Map<String, ? extends Object> componentMap) throws InvalidTopologyException {
        Set<String> keys = componentMap.keySet();
        for (String id : keys) {
            if (Utils.isSystemId(id)) {
                throw new InvalidTopologyException(id + " is not a valid component id.");
            }
        }
        for (Object componentObj : componentMap.values()) {
            ComponentCommon common = getComponentCommon(componentObj);
            Set<String> streamIds = common.get_streams().keySet();
            for (String id : streamIds) {
                if (Utils.isSystemId(id)) {
                    throw new InvalidTopologyException(id + " is not a valid stream id.");
                }
            }
        }
        return keys;
    }
    
    private static void validateIds(StormTopology topology) throws InvalidTopologyException {
        List<String> componentIds = new ArrayList<>();
        componentIds.addAll(validateIds(topology.get_bolts()));
        componentIds.addAll(validateIds(topology.get_spouts()));
        componentIds.addAll(validateIds(topology.get_state_spouts()));

        List<String> offending = Utils.getRepeat(componentIds);
        if (!offending.isEmpty()) {
            throw new InvalidTopologyException("Duplicate component ids: " + offending);
        }
    }

    private static boolean isEmptyInputs(ComponentCommon common) {
        if (common.get_inputs() == null) {
            return true;
        } else {
            return common.get_inputs().isEmpty();
        }
    }

    public static Map<String, Object> allComponents(StormTopology topology) {
        Map<String, Object> components = new HashMap<>(topology.get_bolts());
        components.putAll(topology.get_spouts());
        components.putAll(topology.get_state_spouts());
        return components;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> componentConf(Object component) {
        try {
            Map<String, Object> conf = new HashMap<>();
            ComponentCommon common = getComponentCommon(component);
            String jconf = common.get_json_conf();
            if (jconf != null) {
                conf.putAll((Map<String, Object>) JSONValue.parseWithException(jconf));
            }
            return conf;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static void validateBasic(StormTopology topology) throws InvalidTopologyException {
        validateIds(topology);

        for (StormTopology._Fields field : Thrift.getSpoutFields()) {
            Map<String, Object> spoutComponents = (Map<String, Object>) topology.getFieldValue(field);
            if (spoutComponents != null) {
                for (Object obj : spoutComponents.values()) {
                    ComponentCommon common = getComponentCommon(obj);
                    if (!isEmptyInputs(common)) {
                        throw new InvalidTopologyException("May not declare inputs for a spout");
                    }
                }
            }
        }

        Map<String, Object> componentMap = allComponents(topology);
        for (Object componentObj : componentMap.values()) {
            Map<String, Object> conf = componentConf(componentObj);
            ComponentCommon common = getComponentCommon(componentObj);
            int parallelismHintNum = Thrift.getParallelismHint(common);
            Integer taskNum = ObjectReader.getInt(conf.get(Config.TOPOLOGY_TASKS), 0);
            if (taskNum > 0 && parallelismHintNum <= 0) {
                throw new InvalidTopologyException("Number of executors must be greater than 0 when number of tasks is greater than 0");
            }
        }
    }

    private static Set<String> getStreamOutputFields(Map<String, StreamInfo> streams) {
        Set<String> outputFields = new HashSet<>();
        for (StreamInfo streamInfo : streams.values()) {
            outputFields.addAll(streamInfo.get_output_fields());
        }
        return outputFields;
    }

    public static void validateStructure(StormTopology topology) throws InvalidTopologyException {
        Map<String, Object> componentMap = allComponents(topology);
        for (Map.Entry<String, Object> entry : componentMap.entrySet()) {
            String componentId = entry.getKey();
            ComponentCommon common = getComponentCommon(entry.getValue());
            Map<GlobalStreamId, Grouping> inputs = common.get_inputs();
            for (Map.Entry<GlobalStreamId, Grouping> input : inputs.entrySet()) {
                String sourceStreamId = input.getKey().get_streamId();
                String sourceComponentId = input.getKey().get_componentId();
                if (!componentMap.keySet().contains(sourceComponentId)) {
                    throw new InvalidTopologyException("Component: [" + componentId +
                            "] subscribes from non-existent component [" + sourceComponentId + "]");
                }

                ComponentCommon sourceComponent = getComponentCommon(componentMap.get(sourceComponentId));
                if (!sourceComponent.get_streams().containsKey(sourceStreamId)) {
                    throw new InvalidTopologyException("Component: [" + componentId +
                            "] subscribes from non-existent stream: " +
                            "[" + sourceStreamId + "] of component [" + sourceComponentId + "]");
                }

                Grouping grouping = input.getValue();
                if (Thrift.groupingType(grouping) == Grouping._Fields.FIELDS) {
                    List<String> fields = new ArrayList<>(grouping.get_fields());
                    Map<String, StreamInfo> streams = sourceComponent.get_streams();
                    Set<String> sourceOutputFields = getStreamOutputFields(streams);
                    fields.removeAll(sourceOutputFields);
                    if (fields.size() != 0) {
                        throw new InvalidTopologyException("Component: [" + componentId +
                                "] subscribes from stream: [" + sourceStreamId + "] of component " +
                                "[" + sourceComponentId + "] + with non-existent fields: " + fields);
                    }
                }
            }
        }
    }

    public static Map<GlobalStreamId, Grouping> ackerInputs(StormTopology topology) {
        Map<GlobalStreamId, Grouping> inputs = new HashMap<>();
        Set<String> boltIds = topology.get_bolts().keySet();
        Set<String> spoutIds = topology.get_spouts().keySet();

        for (String id : spoutIds) {
            inputs.put(Utils.getGlobalStreamId(id, Acker.ACKER_INIT_STREAM_ID),
                    Thrift.prepareFieldsGrouping(Arrays.asList("id")));
        }

        for (String id : boltIds) {
            inputs.put(Utils.getGlobalStreamId(id, Acker.ACKER_ACK_STREAM_ID),
                    Thrift.prepareFieldsGrouping(Arrays.asList("id")));
            inputs.put(Utils.getGlobalStreamId(id, Acker.ACKER_FAIL_STREAM_ID),
                    Thrift.prepareFieldsGrouping(Arrays.asList("id")));
            inputs.put(Utils.getGlobalStreamId(id, Acker.ACKER_RESET_TIMEOUT_STREAM_ID),
                    Thrift.prepareFieldsGrouping(Arrays.asList("id")));
        }
        return inputs;
    }

    public static IBolt makeAckerBolt() {
        return _instance.makeAckerBoltImpl();
    }

    public IBolt makeAckerBoltImpl() {
        return new Acker();
    }

    @SuppressWarnings("unchecked")
    public static void addAcker(Map<String, Object> conf, StormTopology topology) {
        int ackerNum = ObjectReader.getInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), ObjectReader.getInt(conf.get(Config.TOPOLOGY_WORKERS)));
        Map<GlobalStreamId, Grouping> inputs = ackerInputs(topology);

        Map<String, StreamInfo> outputStreams = new HashMap<String, StreamInfo>();
        outputStreams.put(Acker.ACKER_ACK_STREAM_ID, Thrift.directOutputFields(Arrays.asList("id", "time-delta-ms")));
        outputStreams.put(Acker.ACKER_FAIL_STREAM_ID, Thrift.directOutputFields(Arrays.asList("id", "time-delta-ms")));
        outputStreams.put(Acker.ACKER_RESET_TIMEOUT_STREAM_ID, Thrift.directOutputFields(Arrays.asList("id", "time-delta-ms")));

        Map<String, Object> ackerConf = new HashMap<>();
        ackerConf.put(Config.TOPOLOGY_TASKS, ackerNum);
        ackerConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, ObjectReader.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)));

        Bolt acker = Thrift.prepareSerializedBoltDetails(inputs, makeAckerBolt(), outputStreams, ackerNum, ackerConf);

        for (Bolt bolt : topology.get_bolts().values()) {
            ComponentCommon common = bolt.get_common();
            common.put_to_streams(Acker.ACKER_ACK_STREAM_ID, Thrift.outputFields(Arrays.asList("id", "ack-val")));
            common.put_to_streams(Acker.ACKER_FAIL_STREAM_ID, Thrift.outputFields(Arrays.asList("id")));
            common.put_to_streams(Acker.ACKER_RESET_TIMEOUT_STREAM_ID, Thrift.outputFields(Arrays.asList("id")));
        }

        for (SpoutSpec spout : topology.get_spouts().values()) {
            ComponentCommon common = spout.get_common();
            Map spoutConf = componentConf(spout);
            spoutConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
                    ObjectReader.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)));
            common.set_json_conf(JSONValue.toJSONString(spoutConf));
            common.put_to_streams(Acker.ACKER_INIT_STREAM_ID,
                    Thrift.outputFields(Arrays.asList("id", "init-val", "spout-task")));
            common.put_to_inputs(Utils.getGlobalStreamId(Acker.ACKER_COMPONENT_ID, Acker.ACKER_ACK_STREAM_ID),
                    Thrift.prepareDirectGrouping());
            common.put_to_inputs(Utils.getGlobalStreamId(Acker.ACKER_COMPONENT_ID, Acker.ACKER_FAIL_STREAM_ID),
                    Thrift.prepareDirectGrouping());
            common.put_to_inputs(Utils.getGlobalStreamId(Acker.ACKER_COMPONENT_ID, Acker.ACKER_RESET_TIMEOUT_STREAM_ID),
                    Thrift.prepareDirectGrouping());
        }

        topology.put_to_bolts(Acker.ACKER_COMPONENT_ID, acker);
    }

    public static ComponentCommon getComponentCommon(Object component) {
        ComponentCommon common = null;
        if (component instanceof StateSpoutSpec) {
            common = ((StateSpoutSpec) component).get_common();
        } else if (component instanceof SpoutSpec) {
            common = ((SpoutSpec) component).get_common();
        } else if (component instanceof Bolt) {
            common = ((Bolt) component).get_common();
        }
        return common;
    }

    public static void addMetricStreams(StormTopology topology) {
        for (Object component : allComponents(topology).values()) {
            ComponentCommon common = getComponentCommon(component);
            StreamInfo streamInfo = Thrift.outputFields(Arrays.asList("task-info", "data-points"));
            common.put_to_streams(Constants.METRICS_STREAM_ID, streamInfo);
        }
    }

    public static void addSystemStreams(StormTopology topology) {
        for (Object component : allComponents(topology).values()) {
            ComponentCommon common = getComponentCommon(component);
            StreamInfo streamInfo = Thrift.outputFields(Arrays.asList("event"));
            common.put_to_streams(SYSTEM_STREAM_ID, streamInfo);
        }
    }

    public static List<String> eventLoggerBoltFields() {
        return Arrays.asList(EventLoggerBolt.FIELD_COMPONENT_ID, EventLoggerBolt.FIELD_MESSAGE_ID,
                EventLoggerBolt.FIELD_TS, EventLoggerBolt.FIELD_VALUES);
    }

    public static Map<GlobalStreamId, Grouping> eventLoggerInputs(StormTopology topology) {
        Map<GlobalStreamId, Grouping> inputs = new HashMap<GlobalStreamId, Grouping>();
        Set<String> allIds = new HashSet<String>();
        allIds.addAll(topology.get_bolts().keySet());
        allIds.addAll(topology.get_spouts().keySet());

        for (String id : allIds) {
            inputs.put(Utils.getGlobalStreamId(id, EVENTLOGGER_STREAM_ID),
                    Thrift.prepareFieldsGrouping(Arrays.asList("component-id")));
        }
        return inputs;
    }

    public static void addEventLogger(Map<String, Object> conf, StormTopology topology) {
        Integer numExecutors = ObjectReader.getInt(conf.get(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS),
                ObjectReader.getInt(conf.get(Config.TOPOLOGY_WORKERS)));
        HashMap<String, Object> componentConf = new HashMap<>();
        componentConf.put(Config.TOPOLOGY_TASKS, numExecutors);
        componentConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, ObjectReader.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)));
        Bolt eventLoggerBolt = Thrift.prepareSerializedBoltDetails(
                eventLoggerInputs(topology), new EventLoggerBolt(), null, numExecutors, componentConf);

        for (Object component : allComponents(topology).values()) {
            ComponentCommon common = getComponentCommon(component);
            common.put_to_streams(EVENTLOGGER_STREAM_ID, Thrift.outputFields(eventLoggerBoltFields()));
        }
        topology.put_to_bolts(EVENTLOGGER_COMPONENT_ID, eventLoggerBolt);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Bolt> metricsConsumerBoltSpecs(Map<String, Object> conf, StormTopology topology) {
        Map<String, Bolt> metricsConsumerBolts = new HashMap<>();

        Set<String> componentIdsEmitMetrics = new HashSet<>();
        componentIdsEmitMetrics.addAll(allComponents(topology).keySet());
        componentIdsEmitMetrics.add(Constants.SYSTEM_COMPONENT_ID);

        Map<GlobalStreamId, Grouping> inputs = new HashMap<>();
        for (String componentId : componentIdsEmitMetrics) {
            inputs.put(Utils.getGlobalStreamId(componentId, Constants.METRICS_STREAM_ID), Thrift.prepareShuffleGrouping());
        }

        List<Map<String, Object>> registerInfo = (List<Map<String, Object>>) conf.get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER);
        if (registerInfo != null) {
            Map<String, Integer> classOccurrencesMap = new HashMap<String, Integer>();
            for (Map<String, Object> info : registerInfo) {
                String className = (String) info.get(TOPOLOGY_METRICS_CONSUMER_CLASS);
                Object argument = info.get(TOPOLOGY_METRICS_CONSUMER_ARGUMENT);
                Integer maxRetainMetricTuples = ObjectReader.getInt(info.get(
                    TOPOLOGY_METRICS_CONSUMER_MAX_RETAIN_METRIC_TUPLES), 100);
                Integer phintNum = ObjectReader.getInt(info.get(TOPOLOGY_METRICS_CONSUMER_PARALLELISM_HINT), 1);
                Map<String, Object> metricsConsumerConf = new HashMap<String, Object>();
                metricsConsumerConf.put(Config.TOPOLOGY_TASKS, phintNum);
                List<String> whitelist = (List<String>) info.get(
                    TOPOLOGY_METRICS_CONSUMER_WHITELIST);
                List<String> blacklist = (List<String>) info.get(
                    TOPOLOGY_METRICS_CONSUMER_BLACKLIST);
                FilterByMetricName filterPredicate = new FilterByMetricName(whitelist, blacklist);
                Boolean expandMapType = ObjectReader.getBoolean(info.get(
                    TOPOLOGY_METRICS_CONSUMER_EXPAND_MAP_TYPE), false);
                String metricNameSeparator = ObjectReader.getString(info.get(
                    TOPOLOGY_METRICS_CONSUMER_METRIC_NAME_SEPARATOR), ".");
                DataPointExpander expander = new DataPointExpander(expandMapType, metricNameSeparator);
                MetricsConsumerBolt boltInstance = new MetricsConsumerBolt(className, argument,
                    maxRetainMetricTuples, filterPredicate, expander);
                Bolt metricsConsumerBolt = Thrift.prepareSerializedBoltDetails(inputs,
                    boltInstance, null, phintNum, metricsConsumerConf);

                String id = className;
                if (classOccurrencesMap.containsKey(className)) {
                    // e.g. [\"a\", \"b\", \"a\"]) => [\"a\", \"b\", \"a#2\"]"
                    int occurrenceNum = classOccurrencesMap.get(className);
                    occurrenceNum++;
                    classOccurrencesMap.put(className, occurrenceNum);
                    id = Constants.METRICS_COMPONENT_ID_PREFIX + className + "#" + occurrenceNum;
                } else {
                    id = Constants.METRICS_COMPONENT_ID_PREFIX + className;
                    classOccurrencesMap.put(className, 1);
                }
                metricsConsumerBolts.put(id, metricsConsumerBolt);
            }
        }
        return metricsConsumerBolts;
    }

    public static void addMetricComponents(Map<String, Object> conf, StormTopology topology) {
        Map<String, Bolt> metricsConsumerBolts = metricsConsumerBoltSpecs(conf, topology);
        for (Map.Entry<String, Bolt> entry : metricsConsumerBolts.entrySet()) {
            topology.put_to_bolts(entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings("unused")
    public static void addSystemComponents(Map<String, Object> conf, StormTopology topology) {
        Map<String, StreamInfo> outputStreams = new HashMap<>();
        outputStreams.put(Constants.SYSTEM_TICK_STREAM_ID, Thrift.outputFields(Arrays.asList("rate_secs")));
        outputStreams.put(Constants.METRICS_TICK_STREAM_ID, Thrift.outputFields(Arrays.asList("interval")));
        outputStreams.put(Constants.CREDENTIALS_CHANGED_STREAM_ID, Thrift.outputFields(Arrays.asList("creds")));

        Map<String, Object> boltConf = new HashMap<>();
        boltConf.put(Config.TOPOLOGY_TASKS, 0);

        Bolt systemBoltSpec = Thrift.prepareSerializedBoltDetails(null, new SystemBolt(), outputStreams, 0, boltConf);
        topology.put_to_bolts(Constants.SYSTEM_COMPONENT_ID, systemBoltSpec);
    }

    public static StormTopology systemTopology(Map<String, Object> topoConf, StormTopology topology) throws InvalidTopologyException {
        return _instance.systemTopologyImpl(topoConf, topology);
    }

    protected StormTopology systemTopologyImpl(Map<String, Object> topoConf, StormTopology topology) throws InvalidTopologyException {
        validateBasic(topology);

        StormTopology ret = topology.deepCopy();
        addAcker(topoConf, ret);
        if (hasEventLoggers(topoConf)) {
            addEventLogger(topoConf, ret);
        }
        addMetricComponents(topoConf, ret);
        addSystemComponents(topoConf, ret);
        addMetricStreams(ret);
        addSystemStreams(ret);

        validateStructure(ret);

        return ret;
    }

    public static boolean hasAckers(Map<String, Object> topoConf) {
        Object ackerNum = topoConf.get(Config.TOPOLOGY_ACKER_EXECUTORS);
        return ackerNum == null || ObjectReader.getInt(ackerNum) > 0;
    }

    public static boolean hasEventLoggers(Map<String, Object> topoConf) {
        Object eventLoggerNum = topoConf.get(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS);
        return eventLoggerNum == null || ObjectReader.getInt(eventLoggerNum) > 0;
    }

    public static int numStartExecutors(Object component) throws InvalidTopologyException {
        ComponentCommon common = getComponentCommon(component);
        return Thrift.getParallelismHint(common);
    }

    public static Map<Integer, String> stormTaskInfo(StormTopology userTopology, Map<String, Object> topoConf) throws InvalidTopologyException {
        return _instance.stormTaskInfoImpl(userTopology, topoConf);
    }

    /*
     * Returns map from task -> componentId
     */
    protected Map<Integer, String> stormTaskInfoImpl(StormTopology userTopology, Map<String, Object> topoConf) throws InvalidTopologyException {
        Map<Integer, String> taskIdToComponentId = new HashMap<>();

        StormTopology systemTopology = systemTopology(topoConf, userTopology);
        Map<String, Object> components = allComponents(systemTopology);
        Map<String, Integer> componentIdToTaskNum = new TreeMap<>();
        for (Map.Entry<String, Object> entry : components.entrySet()) {
            Map<String, Object> conf = componentConf(entry.getValue());
            Object taskNum = conf.get(Config.TOPOLOGY_TASKS);
            componentIdToTaskNum.put(entry.getKey(), ObjectReader.getInt(taskNum));
        }

        int taskId = 1;
        for (Map.Entry<String, Integer> entry : componentIdToTaskNum.entrySet()) {
            String componentId = entry.getKey();
            Integer taskNum = entry.getValue();
            while (taskNum > 0) {
                taskIdToComponentId.put(taskId, componentId);
                taskNum--;
                taskId++;
            }
        }
        return taskIdToComponentId;
    }

    public static List<Integer> executorIdToTasks(List<Long> executorId) {
        List<Integer> taskIds = new ArrayList<>();
        int taskId = executorId.get(0).intValue();
        while (taskId <= executorId.get(1).intValue()) {
            taskIds.add(taskId);
            taskId++;
        }
        return taskIds;
    }

    public static Map<Integer, NodeInfo> taskToNodeport(Map<List<Long>, NodeInfo> executorToNodePort) {
        Map<Integer, NodeInfo> tasksToNodePort = new HashMap<>();
        for (Map.Entry<List<Long>, NodeInfo> entry : executorToNodePort.entrySet()) {
            List<Integer> taskIds = executorIdToTasks(entry.getKey());
            for (Integer taskId : taskIds) {
                tasksToNodePort.put(taskId, entry.getValue());
            }
        }
        return tasksToNodePort;
    }

    public static IAuthorizer mkAuthorizationHandler(String klassName, Map<String, Object> conf)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return _instance.mkAuthorizationHandlerImpl(klassName, conf);
    }

    protected IAuthorizer mkAuthorizationHandlerImpl(String klassName, Map<String, Object> conf)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        IAuthorizer aznHandler = null;
        if (StringUtils.isNotBlank(klassName)) {
            Class<?> aznClass = Class.forName(klassName);
            if (aznClass != null) {
                aznHandler = (IAuthorizer) aznClass.newInstance();
                if (aznHandler != null) {
                    aznHandler.prepare(conf);
                }
                LOG.debug("authorization class name:{}, class:{}, handler:{}", klassName, aznClass, aznHandler);
            }
        }

        return aznHandler;
    }

    @SuppressWarnings("unchecked")
    public static WorkerTopologyContext makeWorkerContext(Map<String, Object> workerData) {
        try {
            StormTopology stormTopology = (StormTopology) workerData.get(Constants.SYSTEM_TOPOLOGY);
            Map<String, Object> topoConf = (Map) workerData.get(Constants.STORM_CONF);
            Map<Integer, String> taskToComponent = (Map<Integer, String>) workerData.get(Constants.TASK_TO_COMPONENT);
            Map<String, List<Integer>> componentToSortedTasks =
                    (Map<String, List<Integer>>) workerData.get(Constants.COMPONENT_TO_SORTED_TASKS);
            Map<String, Map<String, Fields>> componentToStreamToFields =
                    (Map<String, Map<String, Fields>>) workerData.get(Constants.COMPONENT_TO_STREAM_TO_FIELDS);
            String stormId = (String) workerData.get(Constants.STORM_ID);
            Map<String, Object> conf = (Map) workerData.get(Constants.CONF);
            Integer port = (Integer) workerData.get(Constants.PORT);
            String codeDir = ConfigUtils.supervisorStormResourcesPath(ConfigUtils.supervisorStormDistRoot(conf, stormId));
            String pidDir = ConfigUtils.workerPidsRoot(conf, stormId);
            List<Integer> workerTasks = (List<Integer>) workerData.get(Constants.TASK_IDS);
            Map<String, Object> defaultResources = (Map<String, Object>) workerData.get(Constants.DEFAULT_SHARED_RESOURCES);
            Map<String, Object> userResources = (Map<String, Object>) workerData.get(Constants.USER_SHARED_RESOURCES);
            return new WorkerTopologyContext(stormTopology, topoConf, taskToComponent, componentToSortedTasks,
                    componentToStreamToFields, stormId, codeDir, pidDir, port, workerTasks, defaultResources, userResources);
        } catch (IOException e) {
            throw Utils.wrapInRuntime(e);
        }
    }
}
