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
package org.apache.storm.metric;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.metric.util.DataPointExpander;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.RT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


// There is one task inside one executor for each worker of the topology.
// TaskID is always -1, therefore you can only send-unanchored tuples to co-located SystemBolt.
// This bolt was conceived to export worker stats via metrics api.
public class SystemBolt implements IBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SystemBolt.class);

    private static boolean _prepareWasCalled = false;
    private OutputCollector collector;
    private TopologyContext context;
    private DataPointExpander expander;
    private boolean aggregateMode;
    private Map<Integer, Map<Integer, TaskInfoToDataPointsPair>> intervalToTaskToMetricTupleMap;

    private static class TaskInfoToDataPointsPair extends ImmutablePair<IMetricsConsumer.TaskInfo, Collection<IMetricsConsumer.DataPoint>> {
        public TaskInfoToDataPointsPair(IMetricsConsumer.TaskInfo left, Collection<IMetricsConsumer.DataPoint> right) {
            super(left, right);
        }
    }

    private static class ImmutablePair<L, R> {
        private L left;
        private R right;

        public ImmutablePair(L left, R right) {
            this.left = left;
            this.right = right;
        }

        public L getLeft() {
            return left;
        }

        public R getRight() {
            return right;
        }
    }

    private static class MemoryUsageMetric implements IMetric {
        IFn _getUsage;
        public MemoryUsageMetric(IFn getUsage) {
            _getUsage = getUsage;
        }
        @Override
        public Object getValueAndReset() {
            MemoryUsage memUsage = (MemoryUsage)_getUsage.invoke();
            HashMap m = new HashMap();
            m.put("maxBytes", memUsage.getMax());
            m.put("committedBytes", memUsage.getCommitted());
            m.put("initBytes", memUsage.getInit());
            m.put("usedBytes", memUsage.getUsed());
            m.put("virtualFreeBytes", memUsage.getMax() - memUsage.getUsed());
            m.put("unusedBytes", memUsage.getCommitted() - memUsage.getUsed());
            return m;
        }
    }

    // canonically the metrics data exported is time bucketed when doing counts.
    // convert the absolute values here into time buckets.
    private static class GarbageCollectorMetric implements IMetric {
        GarbageCollectorMXBean _gcBean;
        Long _collectionCount;
        Long _collectionTime;
        public GarbageCollectorMetric(GarbageCollectorMXBean gcBean) {
            _gcBean = gcBean;
        }
        @Override
        public Object getValueAndReset() {
            Long collectionCountP = _gcBean.getCollectionCount();
            Long collectionTimeP = _gcBean.getCollectionTime();

            Map ret = null;
            if(_collectionCount!=null && _collectionTime!=null) {
                ret = new HashMap();
                ret.put("count", collectionCountP - _collectionCount);
                ret.put("timeMs", collectionTimeP - _collectionTime);
            }

            _collectionCount = collectionCountP;
            _collectionTime = collectionTimeP;
            return ret;
        }
    }

    @Override
    public void prepare(final Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;

        setupMetricsExpander(stormConf);
        aggregateMode = Utils.getBoolean(stormConf.get(Config.TOPOLOGY_METRICS_AGGREGATE_PER_WORKER), false);

        if(_prepareWasCalled && !"local".equals(stormConf.get(Config.STORM_CLUSTER_MODE))) {
            throw new RuntimeException("A single worker should have 1 SystemBolt instance.");
        }
        _prepareWasCalled = true;

        intervalToTaskToMetricTupleMap = new HashMap<>();

        int bucketSize = RT.intCast(stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));

        final RuntimeMXBean jvmRT = ManagementFactory.getRuntimeMXBean();

        context.registerMetric("uptimeSecs", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return jvmRT.getUptime()/1000.0;
            }
        }, bucketSize);

        context.registerMetric("startTimeSecs", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return jvmRT.getStartTime()/1000.0;
            }
        }, bucketSize);

        context.registerMetric("newWorkerEvent", new IMetric() {
            boolean doEvent = true;

            @Override
            public Object getValueAndReset() {
                if (doEvent) {
                    doEvent = false;
                    return 1;
                } else return 0;
            }
        }, bucketSize);

        final MemoryMXBean jvmMemRT = ManagementFactory.getMemoryMXBean();

        context.registerMetric("memory/heap", new MemoryUsageMetric(new AFn() {
            public Object invoke() {
                return jvmMemRT.getHeapMemoryUsage();
            }
        }), bucketSize);
        context.registerMetric("memory/nonHeap", new MemoryUsageMetric(new AFn() {
            public Object invoke() {
                return jvmMemRT.getNonHeapMemoryUsage();
            }
        }), bucketSize);

        for(GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            context.registerMetric("GC/" + b.getName().replaceAll("\\W", ""), new GarbageCollectorMetric(b), bucketSize);
        }

        registerMetrics(context, (Map<String,String>)stormConf.get(Config.WORKER_METRICS), bucketSize);
        registerMetrics(context, (Map<String,String>)stormConf.get(Config.TOPOLOGY_WORKER_METRICS), bucketSize);
    }

    private void setupMetricsExpander(Map stormConf) {
        boolean expandMapType = Utils.getBoolean(stormConf.get(Config.TOPOLOGY_METRICS_EXPAND_MAP_TYPE), false);
        String metricNameSeparator = Utils.getString(stormConf.get(Config.TOPOLOGY_METRICS_METRIC_NAME_SEPARATOR), ".");
        this.expander = new DataPointExpander(expandMapType, metricNameSeparator);
    }

    private void registerMetrics(TopologyContext context, Map<String, String> metrics, int bucketSize) {
        if (metrics == null) return;
        for (Map.Entry<String, String> metric: metrics.entrySet()) {
            try {
                context.registerMetric(metric.getKey(), (IMetric)Utils.newInstance(metric.getValue()), bucketSize);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void execute(Tuple input) {
        IMetricsConsumer.TaskInfo taskInfo = (IMetricsConsumer.TaskInfo) input.getValue(0);
        Collection<IMetricsConsumer.DataPoint> dataPoints = (Collection) input.getValue(1);
        Collection<IMetricsConsumer.DataPoint> expandedDataPoints = expander.expandDataPoints(dataPoints);

        if (aggregateMode) {
            handleMetricTupleInAggregateMode(taskInfo, expandedDataPoints);
        } else {
            LOG.debug("Emitting metric tuple (no aggregation) - taskInfoEntry: {} / aggregated data points: {}",
                taskInfo, expandedDataPoints);

            collector.emit(Constants.METRICS_AGGREGATE_STREAM_ID, new Values(taskInfo, expandedDataPoints));
        }
    }

    private void handleMetricTupleInAggregateMode(IMetricsConsumer.TaskInfo taskInfo, Collection<IMetricsConsumer.DataPoint> expandedDataPoints) {
        Map<Integer, TaskInfoToDataPointsPair> taskToMetricTupleMap = intervalToTaskToMetricTupleMap.get(taskInfo.updateIntervalSecs);
        if (taskToMetricTupleMap == null) {
            taskToMetricTupleMap = new HashMap<>();
            intervalToTaskToMetricTupleMap.put(taskInfo.updateIntervalSecs, taskToMetricTupleMap);
        }

        LOG.debug("Putting data points for task id: {} / timestamp: {}", taskInfo.srcTaskId, taskInfo.timestamp);

        taskToMetricTupleMap.put(taskInfo.srcTaskId, new TaskInfoToDataPointsPair(taskInfo, expandedDataPoints));

        int currentTimeSec = Time.currentTimeSecs();
        removeOldPendingMetricTuples(taskToMetricTupleMap, currentTimeSec);

        // since we're removing old metric tuples, this means we're OK to aggregate here
        if (isOKtoAggregateMetricsTuples(taskToMetricTupleMap)) {
            LOG.debug("Data points received from all tasks, aggregating and sending");

            Map<IMetricsConsumer.TaskInfo, Collection<IMetricsConsumer.DataPoint>> taskInfoToMetricTupleMap = convertMetricsTupleMapKeyedByTaskInfo(taskToMetricTupleMap, currentTimeSec);

            // let's aggregate by metric name again
            for (Map.Entry<IMetricsConsumer.TaskInfo, Collection<IMetricsConsumer.DataPoint>> taskInfoToDataPointsEntry : taskInfoToMetricTupleMap.entrySet()) {
                IMetricsConsumer.TaskInfo taskInfoEntry = taskInfoToDataPointsEntry.getKey();
                Collection<IMetricsConsumer.DataPoint> dataPointsEntry = taskInfoToDataPointsEntry.getValue();
                Collection<IMetricsConsumer.DataPoint> aggregatedDataPoints = aggregateDataPointsByMetricName(dataPointsEntry);

                LOG.debug("Emitting aggregated metric tuple - taskInfoEntry: {} / aggregated data points: {}", taskInfoEntry, aggregatedDataPoints);

                collector.emit(Constants.METRICS_AGGREGATE_STREAM_ID, new Values(taskInfoEntry, aggregatedDataPoints));
            }

            LOG.debug("Clearing sent metrics");

            // clear out already aggregated metrics
            taskToMetricTupleMap.clear();
        } else {
            LOG.debug("Waiting more tasks metric to aggregate - all tasks: {} / current: {}",
                context.getThisWorkerTasks(), taskToMetricTupleMap.keySet());
        }
    }

    private void removeOldPendingMetricTuples(Map<Integer, TaskInfoToDataPointsPair> taskToMetricTupleMap, int currentTimeSec) {
        // Remove all tuples which is recorded earlier than currentTimeSec - metricEvictSecs
        for(Iterator<Map.Entry<Integer, TaskInfoToDataPointsPair>> it = taskToMetricTupleMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, TaskInfoToDataPointsPair> taskToMetricTupleEntry = it.next();
            TaskInfoToDataPointsPair taskInfoToDataPointsPair = taskToMetricTupleEntry.getValue();
            IMetricsConsumer.TaskInfo taskInfoInEntry = taskInfoToDataPointsPair.getLeft();

            int evictionSecs = taskInfoInEntry.updateIntervalSecs / 2;
            long metricElapsedSecs = currentTimeSec - taskInfoInEntry.timestamp;
            if (metricElapsedSecs > evictionSecs) {
                LOG.debug("Found old pending metric - eviction secs {} but {} secs passed, evicting - taskInfo: {}",
                    evictionSecs, metricElapsedSecs, taskInfoInEntry);
                it.remove();
            }
        }
    }

    private boolean isOKtoAggregateMetricsTuples(Map<Integer, TaskInfoToDataPointsPair> taskToMetricTupleMap) {
        return taskToMetricTupleMap.size() == context.getThisWorkerTasks().size();
    }

    private Map<IMetricsConsumer.TaskInfo, Collection<IMetricsConsumer.DataPoint>> convertMetricsTupleMapKeyedByTaskInfo(Map<Integer, TaskInfoToDataPointsPair> taskToMetricTupleMap, int currentTimeSec) {
        Map<IMetricsConsumer.TaskInfo, Collection<IMetricsConsumer.DataPoint>> taskInfoToDataPointsMap = new HashMap<>();
        for (TaskInfoToDataPointsPair taskInfoToDataPoints : taskToMetricTupleMap.values()) {
            IMetricsConsumer.TaskInfo taskInfoForEntry = taskInfoToDataPoints.getLeft();

            // change task id to system task - the boundary of task id is integer so it is safe
            // also change timestamp to have same value
            // others are same, so it would be effectively keyed by component name
            taskInfoForEntry.srcTaskId = (int) Constants.SYSTEM_TASK_ID;
            taskInfoForEntry.timestamp = currentTimeSec;

            Collection<IMetricsConsumer.DataPoint> taskInfoToDataPointsList = taskInfoToDataPointsMap.get(taskInfoForEntry);
            if (taskInfoToDataPointsList == null) {
                taskInfoToDataPointsList = new ArrayList<>();
                taskInfoToDataPointsMap.put(taskInfoForEntry, taskInfoToDataPointsList);
            }
            taskInfoToDataPointsList.addAll(taskInfoToDataPoints.getRight());
        }
        return taskInfoToDataPointsMap;
    }

    private Collection<IMetricsConsumer.DataPoint> aggregateDataPointsByMetricName(Collection<IMetricsConsumer.DataPoint> dataPoints) {
        Map<String, List<Object>> aggregatedMap = new HashMap<>();
        for (IMetricsConsumer.DataPoint dataPoint : dataPoints) {
            String name = dataPoint.name;
            List<Object> values = aggregatedMap.get(name);
            if (values == null) {
                values = new ArrayList<>();
                aggregatedMap.put(name, values);
            }
            values.add(dataPoint.value);
        }

        Collection<IMetricsConsumer.DataPoint> ret = new ArrayList<>();
        for (Map.Entry<String, List<Object>> nameToDataPoints : aggregatedMap.entrySet()) {
            IMetricsConsumer.DataPoint dataPoint = new IMetricsConsumer.DataPoint(nameToDataPoints.getKey(), nameToDataPoints.getValue());
            ret.add(dataPoint);
        }

        return ret;
    }

    @Override
    public void cleanup() {
    }
}
