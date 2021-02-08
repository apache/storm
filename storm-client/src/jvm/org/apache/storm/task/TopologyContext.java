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

package org.apache.storm.task;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.metric.api.CombinedMetric;
import org.apache.storm.metric.api.ICombiner;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.shade.org.apache.commons.lang.NotImplementedException;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.state.ISubscribedState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * A `TopologyContext` is given to bolts and spouts in their `prepare()` and `open()` methods, respectively. This object provides
 * information about the component's place within the topology, such as task ids, inputs and outputs, etc.
 *
 * <p>The `TopologyContext` is also used to declare `ISubscribedState` objects to synchronize state with StateSpouts
 * this object is subscribed to.
 */
public class TopologyContext extends WorkerTopologyContext implements IMetricsContext {
    private final Integer taskId;
    private final Map<String, Object> taskData = new HashMap<>();
    private final List<ITaskHook> hooks = new ArrayList<>();
    private final Map<String, Object> executorData;
    private final Map<Integer, Map<Integer, Map<String, IMetric>>> registeredMetrics;
    private final AtomicBoolean openOrPrepareWasCalled;
    private final StormMetricRegistry metricRegistry;
    // This is updated by the Worker and the topology has shared access to it
    private final Map<String, Long> blobToLastKnownVersion;

    public TopologyContext(StormTopology topology,
                           Map<String, Object> topoConf,
                           Map<Integer, String> taskToComponent,
                           Map<String, List<Integer>> componentToSortedTasks,
                           Map<String, Map<String, Fields>> componentToStreamToFields,
                           Map<String, Long> blobToLastKnownVersionShared,
                           String stormId,
                           String codeDir,
                           String pidDir,
                           Integer taskId,
                           Integer workerPort,
                           List<Integer> workerTasks,
                           Map<String, Object> defaultResources,
                           Map<String, Object> userResources,
                           Map<String, Object> executorData,
                           Map<Integer, Map<Integer, Map<String, IMetric>>> registeredMetrics,
                           AtomicBoolean openOrPrepareWasCalled,
                           StormMetricRegistry metricRegistry) {
        super(topology, topoConf, taskToComponent, componentToSortedTasks,
              componentToStreamToFields, stormId, codeDir, pidDir,
              workerPort, workerTasks, defaultResources, userResources);
        this.metricRegistry = metricRegistry;
        this.taskId = taskId;
        this.executorData = executorData;
        this.registeredMetrics = registeredMetrics;
        this.openOrPrepareWasCalled = openOrPrepareWasCalled;
        blobToLastKnownVersion = blobToLastKnownVersionShared;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    private static Map<String, Object> groupingToJSONableMap(Grouping grouping) {
        Map<String, Object> groupingMap = new HashMap<>();
        groupingMap.put("type", grouping.getSetField().toString());
        if (grouping.is_set_fields()) {
            groupingMap.put("fields", grouping.get_fields());
        }
        return groupingMap;
    }

    /**
     * All state from all subscribed state spouts streams will be synced with the provided object.
     *
     * <p>It is recommended that your ISubscribedState object is kept as an instance variable of this object. The recommended usage of this
     * method is as follows:
     *
     * <p>```java _myState = context.setAllSubscribedState(new MyState()); ```
     *
     * @param obj Provided ISubscribedState implementation
     * @return Returns the ISubscribedState object provided
     */
    public <T extends ISubscribedState> T setAllSubscribedState(T obj) {
        //check that only subscribed to one component/stream for statespout
        //setsubscribedstate appropriately
        throw new NotImplementedException();
    }

    /**
     * Synchronizes the default stream from the specified state spout component id with the provided ISubscribedState object.
     *
     * <p>The recommended usage of this method is as follows:
     *
     * <p>```java _myState = context.setSubscribedState(componentId, new MyState()); ```
     *
     * @param componentId the id of the StateSpout component to subscribe to
     * @param obj         Provided ISubscribedState implementation
     * @return Returns the ISubscribedState object provided
     */
    public <T extends ISubscribedState> T setSubscribedState(String componentId, T obj) {
        return setSubscribedState(componentId, Utils.DEFAULT_STREAM_ID, obj);
    }

    /**
     * Synchronizes the specified stream from the specified state spout component id with the provided ISubscribedState object.
     *
     * <p>The recommended usage of this method is as follows:
     *
     * <p>```java _myState = context.setSubscribedState(componentId, streamId, new MyState()); ```
     *
     * @param componentId the id of the StateSpout component to subscribe to
     * @param streamId    the stream to subscribe to
     * @param obj         Provided ISubscribedState implementation
     * @return Returns the ISubscribedState object provided
     */
    public <T extends ISubscribedState> T setSubscribedState(String componentId, String streamId, T obj) {
        throw new NotImplementedException();
    }

    public Map<String, Long> getBlobToLastKnownVersion() {
        return blobToLastKnownVersion;
    }

    /**
     * Gets the task id of this task.
     *
     * @return the task id
     */
    public int getThisTaskId() {
        return taskId;
    }

    /**
     * Get component id.
     * @return the component id for this task. The component id maps to a component id specified for a Spout or Bolt in the topology
     *     definition.
     */
    public String getThisComponentId() {
        return getComponentId(taskId);
    }

    /**
     * Gets the declared output fields for the specified stream id for the component this task is a part of.
     */
    public Fields getThisOutputFields(String streamId) {
        return getComponentOutputFields(getThisComponentId(), streamId);
    }

    /**
     * Gets the declared output fields for all streams for the component this task is a part of.
     */
    public Map<String, List<String>> getThisOutputFieldsForStreams() {
        Map<String, List<String>> streamToFields = new HashMap<>();
        for (String stream : this.getThisStreams()) {
            streamToFields.put(stream, this.getThisOutputFields(stream).toList());
        }
        return streamToFields;
    }

    /**
     * Gets the set of streams declared for the component of this task.
     */
    public Set<String> getThisStreams() {
        return getComponentStreams(getThisComponentId());
    }

    /**
     * Gets the index of this task id in getComponentTasks(getThisComponentId()). An example use case for this method is determining which
     * task accesses which resource in a distributed resource to ensure an even distribution.
     */
    public int getThisTaskIndex() {
        List<Integer> tasks = new ArrayList<>(getComponentTasks(getThisComponentId()));
        Collections.sort(tasks);
        for (int i = 0; i < tasks.size(); i++) {
            if (tasks.get(i) == getThisTaskId()) {
                return i;
            }
        }
        throw new RuntimeException("Fatal: could not find this task id in this component");
    }

    /**
     * Gets the declared input fields for this component.
     *
     * @return A map from sources to streams to fields.
     */
    public Map<String, Map<String, List<String>>> getThisInputFields() {
        Map<String, Map<String, List<String>>> outputMap = new HashMap<>();
        for (Map.Entry<GlobalStreamId, Grouping> entry : this.getThisSources().entrySet()) {
            String componentId = entry.getKey().get_componentId();
            Set<String> streams = getComponentStreams(componentId);
            for (String stream : streams) {
                Map<String, List<String>> streamFieldMap = outputMap.get(componentId);
                if (streamFieldMap == null) {
                    streamFieldMap = new HashMap<>();
                    outputMap.put(componentId, streamFieldMap);
                }
                streamFieldMap.put(stream, getComponentOutputFields(componentId, stream).toList());
            }
        }
        return outputMap;
    }

    /**
     * Gets the declared inputs to this component.
     *
     * @return A map from subscribed component/stream to the grouping subscribed with.
     */
    public Map<GlobalStreamId, Grouping> getThisSources() {
        return getSources(getThisComponentId());
    }

    /**
     * Gets information about who is consuming the outputs of this component, and how.
     *
     * @return Map from stream id to component id to the Grouping used.
     */
    public Map<String, Map<String, Grouping>> getThisTargets() {
        return getTargets(getThisComponentId());
    }

    public void setTaskData(String name, Object data) {
        taskData.put(name, data);
    }

    public Object getTaskData(String name) {
        return taskData.get(name);
    }

    public void setExecutorData(String name, Object data) {
        executorData.put(name, data);
    }

    public Object getExecutorData(String name) {
        return executorData.get(name);
    }

    public void addTaskHook(ITaskHook hook) {
        hook.prepare(topoConf, this);
        hooks.add(hook);
    }

    public List<ITaskHook> getHooks() {
        return hooks;
    }

    @Override
    public String toJSONString() {
        Map<String, Object> obj = new HashMap<>();
        obj.put("task->component", this.getTaskToComponent());
        obj.put("taskid", this.getThisTaskId());
        obj.put("componentid", this.getThisComponentId());
        List<String> streamList = new ArrayList<>();
        streamList.addAll(this.getThisStreams());
        obj.put("streams", streamList);
        obj.put("stream->outputfields", this.getThisOutputFieldsForStreams());
        // Convert targets to a JSON serializable format
        Map<String, Map<String, Object>> stringTargets = new HashMap<>();
        for (Map.Entry<String, Map<String, Grouping>> entry : this.getThisTargets().entrySet()) {
            Map<String, Object> stringTargetMap = new HashMap<>();
            for (Map.Entry<String, Grouping> innerEntry : entry.getValue().entrySet()) {
                stringTargetMap.put(innerEntry.getKey(), groupingToJSONableMap(innerEntry.getValue()));
            }
            stringTargets.put(entry.getKey(), stringTargetMap);
        }
        obj.put("stream->target->grouping", stringTargets);
        // Convert sources to a JSON serializable format
        Map<String, Map<String, Object>> stringSources = new HashMap<>();
        for (Map.Entry<GlobalStreamId, Grouping> entry : this.getThisSources().entrySet()) {
            GlobalStreamId gid = entry.getKey();
            Map<String, Object> stringSourceMap = stringSources.get(gid.get_componentId());
            if (stringSourceMap == null) {
                stringSourceMap = new HashMap<>();
                stringSources.put(gid.get_componentId(), stringSourceMap);
            }
            stringSourceMap.put(gid.get_streamId(), groupingToJSONableMap(entry.getValue()));
        }
        obj.put("source->stream->grouping", stringSources);
        obj.put("source->stream->fields", this.getThisInputFields());
        return JSONValue.toJSONString(obj);
    }

    /*
     * Register a IMetric instance.
     *
     * Storm will then call `getValueAndReset()` on the metric every `timeBucketSizeInSecs`
     * and the returned value is sent to all metrics consumers.
     *
     * You must call this during `IBolt.prepare()` or `ISpout.open()`.
     * @return The IMetric argument unchanged.
     */
    @Deprecated
    @Override
    public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs) {
        if (openOrPrepareWasCalled.get()) {
            throw new RuntimeException("TopologyContext.registerMetric can only be called from within overridden "
                    + "IBolt::prepare() or ISpout::open() method.");
        }

        if (metric == null) {
            throw new IllegalArgumentException("Cannot register a null metric");
        }

        if (timeBucketSizeInSecs <= 0) {
            throw new IllegalArgumentException("TopologyContext.registerMetric can only be called with "
                    + "timeBucketSizeInSecs greater than or equal to 1 second.");
        }

        if (getRegisteredMetricByName(name) != null) {
            throw new RuntimeException("The same metric name `" + name + "` was registered twice.");
        }

        Map<Integer, Map<Integer, Map<String, IMetric>>> m1 = registeredMetrics;
        if (!m1.containsKey(timeBucketSizeInSecs)) {
            m1.put(timeBucketSizeInSecs, new HashMap<Integer, Map<String, IMetric>>());
        }

        Map<Integer, Map<String, IMetric>> m2 = m1.get(timeBucketSizeInSecs);
        if (!m2.containsKey(taskId)) {
            m2.put(taskId, new HashMap<String, IMetric>());
        }

        Map<String, IMetric> m3 = m2.get(taskId);
        if (m3.containsKey(name)) {
            throw new RuntimeException("The same metric name `" + name + "` was registered twice.");
        } else {
            m3.put(name, metric);
        }

        return metric;
    }

    /*
     * Convenience method for registering ReducedMetric.
     */
    @Deprecated
    @Override
    public ReducedMetric registerMetric(String name, IReducer reducer, int timeBucketSizeInSecs) {
        return registerMetric(name, new ReducedMetric(reducer), timeBucketSizeInSecs);
    }

    /*
     * Convenience method for registering CombinedMetric.
     */
    @Deprecated
    @Override
    public CombinedMetric registerMetric(String name, ICombiner combiner, int timeBucketSizeInSecs) {
        return registerMetric(name, new CombinedMetric(combiner), timeBucketSizeInSecs);
    }

    /**
     * Get component's metric from registered metrics by name. Notice: Normally, one component can only register one metric name once. But
     * now registerMetric has a bug(https://issues.apache.org/jira/browse/STORM-254) cause the same metric name can register twice. So we
     * just return the first metric we meet.
     */
    @Deprecated
    public IMetric getRegisteredMetricByName(String name) {
        IMetric metric = null;

        for (Map<Integer, Map<String, IMetric>> taskIdToNameToMetric : registeredMetrics.values()) {
            Map<String, IMetric> nameToMetric = taskIdToNameToMetric.get(taskId);
            if (nameToMetric != null) {
                metric = nameToMetric.get(name);
                if (metric != null) {
                    //we just return the first metric we meet
                    break;
                }
            }
        }

        return metric;
    }

    @Override
    public Timer registerTimer(String name) {
        return metricRegistry.timer(name, this);
    }

    @Override
    public Histogram registerHistogram(String name) {
        return metricRegistry.histogram(name, this);
    }

    @Override
    public Meter registerMeter(String name) {
        return metricRegistry.meter(name, this);
    }

    @Override
    public Counter registerCounter(String name) {
        return metricRegistry.counter(name, this);
    }

    @Override
    public <T> Gauge<T> registerGauge(String name, Gauge<T> gauge) {
        return metricRegistry.gauge(name, gauge, this);
    }

    @Override
    public void registerMetricSet(String prefix, MetricSet set) {
        metricRegistry.metricSet(prefix, set, this);
    }
}
