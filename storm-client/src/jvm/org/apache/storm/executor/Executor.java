/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.executor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.StormTimer;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.GrouperFactory;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.executor.bolt.BoltExecutor;
import org.apache.storm.executor.error.IReportError;
import org.apache.storm.executor.error.ReportError;
import org.apache.storm.executor.error.ReportErrorAndDie;
import org.apache.storm.executor.spout.SpoutExecutor;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.LoadAwareCustomStreamGrouping;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.metrics2.PerReporterGauge;
import org.apache.storm.metrics2.RateCounter;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.shade.org.jctools.queues.MpscChunkedArrayQueue;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.stats.ClientStatsUtil;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Executor implements Callable, JCQueue.Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(Executor.class);

    protected final WorkerState workerData;
    protected final WorkerTopologyContext workerTopologyContext;
    protected final List<Long> executorId;
    protected final List<Integer> taskIds;
    protected final String componentId;
    protected final AtomicBoolean openOrPrepareWasCalled;
    protected final Map<String, Object> topoConf;
    protected final Map<String, Object> conf;
    protected final String stormId;
    protected final HashMap sharedExecutorData;
    protected final CountDownLatch workerReady;
    protected final AtomicBoolean stormActive;
    protected final AtomicReference<Map<String, DebugOptions>> stormComponentDebug;
    protected final Runnable suicideFn;
    protected final IStormClusterState stormClusterState;
    protected final Map<Integer, String> taskToComponent;
    protected final Map<Integer, Map<Integer, Map<String, IMetric>>> intervalToTaskToMetricToRegistry;
    protected final Map<String, Map<String, LoadAwareCustomStreamGrouping>> streamToComponentToGrouper;
    protected final List<LoadAwareCustomStreamGrouping> groupers;
    protected final ReportErrorAndDie reportErrorDie;
    protected final BooleanSupplier sampler;
    protected final String type;
    protected final IReportError reportError;
    protected final Random rand;
    protected final JCQueue receiveQueue;
    protected final Map<String, String> credentials;
    protected final Boolean isDebug;
    protected final Boolean hasEventLoggers;
    protected final boolean ackingEnabled;
    protected final MpscChunkedArrayQueue<AddressedTuple> pendingEmits = new MpscChunkedArrayQueue<>(1024, (int) Math.pow(2, 30));
    private final AddressedTuple flushTuple;
    protected ExecutorTransfer executorTransfer;
    protected ArrayList<Task> idToTask;
    protected int idToTaskBase;
    protected String hostname;
    private static final double msDurationFactor = 1.0 / TimeUnit.MILLISECONDS.toNanos(1);
    private AtomicBoolean needToRefreshCreds = new AtomicBoolean(false);
    private final RateCounter reportedErrorCount;
    private final boolean enableV2MetricsDataPoints;
    private final Integer v2MetricsTickInterval;

    protected Executor(WorkerState workerData, List<Long> executorId, Map<String, String> credentials, String type) {
        this.workerData = workerData;
        this.executorId = executorId;
        this.type = type;
        this.workerTopologyContext = workerData.getWorkerTopologyContext();
        this.taskIds = StormCommon.executorIdToTasks(executorId);
        this.componentId = workerTopologyContext.getComponentId(taskIds.get(0));
        this.openOrPrepareWasCalled = new AtomicBoolean(false);
        this.topoConf = normalizedComponentConf(workerData.getTopologyConf(), workerTopologyContext, componentId);
        this.receiveQueue = (workerData.getExecutorReceiveQueueMap().get(executorId));
        this.stormId = workerData.getTopologyId();
        this.conf = workerData.getConf();
        this.sharedExecutorData = new HashMap();
        this.workerReady = workerData.getIsWorkerActive();
        this.stormActive = workerData.getIsTopologyActive();
        this.stormComponentDebug = workerData.getStormComponentToDebug();

        this.executorTransfer = new ExecutorTransfer(workerData, topoConf);

        this.suicideFn = workerData.getSuicideCallback();
        try {
            this.stormClusterState = ClusterUtils.mkStormClusterState(workerData.getStateStorage(),
                                                                      new ClusterStateContext(DaemonType.WORKER, topoConf));
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }

        this.intervalToTaskToMetricToRegistry = new HashMap<>();
        this.taskToComponent = workerData.getTaskToComponent();
        this.streamToComponentToGrouper = outboundComponents(workerTopologyContext, componentId, topoConf);
        if (this.streamToComponentToGrouper != null) {
            this.groupers = streamToComponentToGrouper.values().stream()
                                                      .filter(Objects::nonNull)
                                                      .flatMap(m -> m.values().stream()).collect(Collectors.toList());
        } else {
            this.groupers = Collections.emptyList();
        }
        this.reportError = new ReportError(topoConf, stormClusterState, stormId, componentId, workerTopologyContext);
        this.reportErrorDie = new ReportErrorAndDie(reportError, suicideFn);
        this.sampler = ConfigUtils.mkStatsSampler(topoConf);
        this.isDebug = ObjectReader.getBoolean(topoConf.get(Config.TOPOLOGY_DEBUG), false);
        this.rand = new Random(Utils.secureRandomLong());
        this.credentials = credentials;
        this.hasEventLoggers = StormCommon.hasEventLoggers(topoConf);
        this.ackingEnabled = StormCommon.hasAckers(topoConf);

        try {
            this.hostname = Utils.hostname();
        } catch (UnknownHostException ignored) {
            this.hostname = "";
        }
        flushTuple = AddressedTuple.createFlushTuple(workerTopologyContext);
        this.reportedErrorCount = workerData.getMetricRegistry().rateCounter("__reported-error-count", componentId,
                taskIds.get(0));

        enableV2MetricsDataPoints = ObjectReader.getBoolean(topoConf.get(Config.TOPOLOGY_ENABLE_V2_METRICS_TICK), false);
        v2MetricsTickInterval = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_V2_METRICS_TICK_INTERVAL_SECONDS), 60);
    }

    public static Executor mkExecutor(WorkerState workerState, List<Long> executorId, Map<String, String> credentials) {
        Executor executor;

        WorkerTopologyContext workerTopologyContext = workerState.getWorkerTopologyContext();
        List<Integer> taskIds = StormCommon.executorIdToTasks(executorId);
        String componentId = workerTopologyContext.getComponentId(taskIds.get(0));

        String type = getExecutorType(workerTopologyContext, componentId);
        if (ClientStatsUtil.SPOUT.equals(type)) {
            executor = new SpoutExecutor(workerState, executorId, credentials);
        } else {
            executor = new BoltExecutor(workerState, executorId, credentials);
        }

        int minId = Integer.MAX_VALUE;
        Map<Integer, Task> idToTask = new HashMap<>();
        for (Integer taskId : taskIds) {
            minId = Math.min(minId, taskId);
            try {
                Task task = new Task(executor, taskId);
                idToTask.put(taskId, task);
            } catch (IOException ex) {
                throw Utils.wrapInRuntime(ex);
            }
        }

        executor.idToTaskBase = minId;
        executor.idToTask = Utils.convertToArray(idToTask, minId);
        return executor;
    }

    private static String getExecutorType(WorkerTopologyContext workerTopologyContext, String componentId) {
        StormTopology topology = workerTopologyContext.getRawTopology();
        Map<String, SpoutSpec> spouts = topology.get_spouts();
        Map<String, Bolt> bolts = topology.get_bolts();
        if (spouts.containsKey(componentId)) {
            return ClientStatsUtil.SPOUT;
        } else if (bolts.containsKey(componentId)) {
            return ClientStatsUtil.BOLT;
        } else {
            throw new RuntimeException("Could not find " + componentId + " in " + topology);
        }
    }

    /**
     * Retrieves all values of all static fields of {@link Config} which represent all available configuration keys through reflection. The
     * method assumes that they are {@code String}s through reflection.
     *
     * @return the list of retrieved field values
     *
     * @throws ClassCastException if one of the fields is not of type {@code String}
     */
    private static List<String> retrieveAllConfigKeys() {
        List<String> ret = new ArrayList<>();
        Field[] fields = Config.class.getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                String fieldValue = (String) fields[i].get(null);
                ret.add(fieldValue);
            } catch (IllegalArgumentException e) {
                LOG.error(e.getMessage(), e);
            } catch (IllegalAccessException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return ret;
    }

    public Queue<AddressedTuple> getPendingEmits() {
        return pendingEmits;
    }

    /**
     * separated from mkExecutor in order to replace executor transfer in executor data for testing.
     */
    public ExecutorShutdown execute() throws Exception {
        LOG.info("Loading executor tasks " + componentId + ":" + executorId);

        String handlerName = componentId + "-executor" + executorId;
        Utils.SmartThread handler =
            Utils.asyncLoop(this, false, reportErrorDie, Thread.NORM_PRIORITY, true, true, handlerName);

        LOG.info("Finished loading executor " + componentId + ":" + executorId);
        return new ExecutorShutdown(this, Lists.newArrayList(handler), idToTask, receiveQueue);
    }

    public abstract void tupleActionFn(int taskId, TupleImpl tuple) throws Exception;

    @Override
    public void accept(Object event) {
        AddressedTuple addressedTuple = (AddressedTuple) event;
        int taskId = addressedTuple.getDest();

        TupleImpl tuple = (TupleImpl) addressedTuple.getTuple();
        if (isDebug) {
            LOG.info("Processing received TUPLE: {} for TASK: {} ", tuple, taskId);
        }

        try {
            if (taskId != AddressedTuple.BROADCAST_DEST) {
                tupleActionFn(taskId, tuple);
            } else {
                for (Integer t : taskIds) {
                    tupleActionFn(t, tuple);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setNeedToRefreshCreds() {
        this.needToRefreshCreds.set(true);
    }

    protected void updateExecCredsIfRequired() {
        if (this.needToRefreshCreds.get()) {
            this.needToRefreshCreds.set(false);
            LOG.info("The credentials are being updated {}.", executorId);
            Credentials creds = this.workerData.getCredentials();
            idToTask.stream().map(Task::getTaskObject).filter(taskObject -> taskObject instanceof ICredentialsListener)
                    .forEach(taskObject -> {
                        ((ICredentialsListener) taskObject).setCredentials(creds == null ? null : creds.get_creds());
                    });
        }
    }

    @Override
    public void flush() {
        // NO-OP
    }

    public void metricsTick(Task task, TupleImpl tuple) {
        try {
            Integer interval = tuple.getInteger(0);
            int taskId = task.getTaskId();
            Map<Integer, Map<String, IMetric>> taskToMetricToRegistry = intervalToTaskToMetricToRegistry.get(interval);
            Map<String, IMetric> nameToRegistry = null;
            if (taskToMetricToRegistry != null) {
                nameToRegistry = taskToMetricToRegistry.get(taskId);
            }
            List<IMetricsConsumer.DataPoint> dataPoints = new ArrayList<>();
            if (nameToRegistry != null) {
                for (Map.Entry<String, IMetric> entry : nameToRegistry.entrySet()) {
                    IMetric metric = entry.getValue();
                    Object value = metric.getValueAndReset();
                    if (value != null) {
                        IMetricsConsumer.DataPoint dataPoint = new IMetricsConsumer.DataPoint(entry.getKey(), value);
                        dataPoints.add(dataPoint);
                    }
                }
            }
            addV2Metrics(taskId, dataPoints, interval);

            if (!dataPoints.isEmpty()) {
                IMetricsConsumer.TaskInfo taskInfo = new IMetricsConsumer.TaskInfo(
                        hostname, workerTopologyContext.getThisWorkerPort(),
                        componentId, taskId, Time.currentTimeSecs(), interval);
                task.sendUnanchored(Constants.METRICS_STREAM_ID,
                        new Values(taskInfo, dataPoints), executorTransfer, pendingEmits);
                executorTransfer.flush();
            }
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    // updates v1 metric dataPoints with v2 metric API data
    private void addV2Metrics(int taskId, List<IMetricsConsumer.DataPoint> dataPoints, int interval) {
        if (!enableV2MetricsDataPoints) {
            return;
        }

        // only report v2 metric on the proper metrics tick interval
        if (interval != v2MetricsTickInterval) {
            return;
        }

        processGauges(taskId, dataPoints);
        processCounters(taskId, dataPoints);
        processHistograms(taskId, dataPoints);
        processMeters(taskId, dataPoints);
        processTimers(taskId, dataPoints);
    }

    private void processGauges(int taskId, List<IMetricsConsumer.DataPoint> dataPoints) {
        Map<String, Gauge> gauges = workerData.getMetricRegistry().getTaskGauges(taskId);
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            Gauge gauge = entry.getValue();
            Object v;
            if (gauge instanceof PerReporterGauge) {
                v = ((PerReporterGauge) gauge).getValueForReporter(this);
            } else {
                v = gauge.getValue();
            }
            if (v instanceof Number) {
                IMetricsConsumer.DataPoint dataPoint = new IMetricsConsumer.DataPoint(entry.getKey(), v);
                dataPoints.add(dataPoint);
            }
        }
    }

    private void processCounters(int taskId, List<IMetricsConsumer.DataPoint> dataPoints) {
        Map<String, Counter> counters = workerData.getMetricRegistry().getTaskCounters(taskId);
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            Object value = entry.getValue().getCount();
            IMetricsConsumer.DataPoint dataPoint = new IMetricsConsumer.DataPoint(entry.getKey(), value);
            dataPoints.add(dataPoint);
        }
    }

    private void processHistograms(int taskId, List<IMetricsConsumer.DataPoint> dataPoints) {
        Map<String, Histogram> histograms = workerData.getMetricRegistry().getTaskHistograms(taskId);
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            Snapshot snapshot =  entry.getValue().getSnapshot();
            addSnapshotDatapoints(entry.getKey(), snapshot, dataPoints);
            IMetricsConsumer.DataPoint dataPoint = new IMetricsConsumer.DataPoint(entry.getKey() + ".count", entry.getValue().getCount());
            dataPoints.add(dataPoint);
        }
    }

    private void processMeters(int taskId, List<IMetricsConsumer.DataPoint> dataPoints) {
        Map<String, Meter> meters = workerData.getMetricRegistry().getTaskMeters(taskId);
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            addMeteredDatapoints(entry.getKey(), entry.getValue(), dataPoints);
        }
    }

    private void processTimers(int taskId, List<IMetricsConsumer.DataPoint> dataPoints) {
        Map<String, Timer> timers = workerData.getMetricRegistry().getTaskTimers(taskId);
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            Snapshot snapshot =  entry.getValue().getSnapshot();
            addSnapshotDatapoints(entry.getKey(), snapshot, dataPoints);
            addMeteredDatapoints(entry.getKey(), entry.getValue(), dataPoints);
        }
    }

    private void addMeteredDatapoints(String baseName, Metered metered, List<IMetricsConsumer.DataPoint> dataPoints) {
        IMetricsConsumer.DataPoint dataPoint = new IMetricsConsumer.DataPoint(baseName + ".count", metered.getCount());
        dataPoints.add(dataPoint);
        addConvertedMetric(baseName, ".m1_rate", metered.getOneMinuteRate(), dataPoints, false);
        addConvertedMetric(baseName, ".m5_rate", metered.getFiveMinuteRate(), dataPoints, false);
        addConvertedMetric(baseName, ".m15_rate", metered.getFifteenMinuteRate(), dataPoints, false);
        addConvertedMetric(baseName, ".mean_rate", metered.getMeanRate(), dataPoints, false);
    }

    private void addSnapshotDatapoints(String baseName, Snapshot snapshot, List<IMetricsConsumer.DataPoint> dataPoints) {
        addConvertedMetric(baseName, ".max", snapshot.getMax(), dataPoints, true);
        addConvertedMetric(baseName, ".mean", snapshot.getMean(), dataPoints, true);
        addConvertedMetric(baseName, ".min", snapshot.getMin(), dataPoints, true);
        addConvertedMetric(baseName, ".stddev", snapshot.getStdDev(), dataPoints, true);
        addConvertedMetric(baseName, ".p50", snapshot.getMedian(), dataPoints, true);
        addConvertedMetric(baseName, ".p75", snapshot.get75thPercentile(), dataPoints, true);
        addConvertedMetric(baseName, ".p95", snapshot.get95thPercentile(), dataPoints, true);
        addConvertedMetric(baseName, ".p98", snapshot.get98thPercentile(), dataPoints, true);
        addConvertedMetric(baseName, ".p99", snapshot.get99thPercentile(), dataPoints, true);
        addConvertedMetric(baseName, ".p999", snapshot.get999thPercentile(), dataPoints, true);
    }

    private void addConvertedMetric(String baseName, String suffix, double value,
                                    List<IMetricsConsumer.DataPoint> dataPoints, boolean needConversion) {
        IMetricsConsumer.DataPoint dataPoint
            = new IMetricsConsumer.DataPoint(baseName + suffix, needConversion ? convertDuration(value) : value);
        dataPoints.add(dataPoint);
    }

    // converts timed codahale metric values from nanosecond to millisecond time scale
    private double convertDuration(double duration) {
        return duration * msDurationFactor;
    }

    protected void setupMetrics() {
        boolean v2TickScheduled = !enableV2MetricsDataPoints;
        for (final Integer interval : intervalToTaskToMetricToRegistry.keySet()) {
            scheduleMetricsTick(interval);
            if (interval == v2MetricsTickInterval) {
                v2TickScheduled = true;
            }
        }

        if (!v2TickScheduled) {
            LOG.info("Scheduling v2 metrics tick for interval {}", v2MetricsTickInterval);
            scheduleMetricsTick(v2MetricsTickInterval);
        }
    }

    private void scheduleMetricsTick(int interval) {
        StormTimer timerTask = workerData.getUserTimer();
        timerTask.scheduleRecurring(interval, interval,
            () -> {
                TupleImpl tuple =
                        new TupleImpl(workerTopologyContext, new Values(interval), Constants.SYSTEM_COMPONENT_ID,
                                (int) Constants.SYSTEM_TASK_ID, Constants.METRICS_TICK_STREAM_ID);
                AddressedTuple metricsTickTuple = new AddressedTuple(AddressedTuple.BROADCAST_DEST, tuple);
                try {
                    receiveQueue.publish(metricsTickTuple);
                    receiveQueue.flush();  // avoid buffering
                } catch (InterruptedException e) {
                    LOG.warn("Thread interrupted when publishing metrics. Setting interrupt flag.");
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        );
    }

    protected void setupTicks(boolean isSpout) {
        final Integer tickTimeSecs = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS), null);
        if (tickTimeSecs != null) {
            boolean enableMessageTimeout = (Boolean) topoConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS);
            boolean isAcker = Acker.ACKER_COMPONENT_ID.equals(componentId);
            if ((!isAcker && Utils.isSystemId(componentId))
                || (!enableMessageTimeout && isSpout)
                || (!enableMessageTimeout && isAcker)) {
                LOG.info("Timeouts disabled for executor {}:{}", componentId, executorId);
            } else {
                StormTimer timerTask = workerData.getUserTimer();
                timerTask.scheduleRecurring(tickTimeSecs, tickTimeSecs,
                    () -> {
                        TupleImpl tuple = new TupleImpl(workerTopologyContext, new Values(tickTimeSecs),
                                                        Constants.SYSTEM_COMPONENT_ID,
                                                        (int) Constants.SYSTEM_TASK_ID,
                                                        Constants.SYSTEM_TICK_STREAM_ID);
                        AddressedTuple tickTuple = new AddressedTuple(AddressedTuple.BROADCAST_DEST, tuple);
                        try {
                            receiveQueue.publish(tickTuple);
                            receiveQueue.flush(); // avoid buffering
                        } catch (InterruptedException e) {
                            LOG.warn("Thread interrupted when emitting tick tuple. Setting interrupt flag.");
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                );
            }
        }
    }

    public void reflectNewLoadMapping(LoadMapping loadMapping) {
        for (LoadAwareCustomStreamGrouping g : groupers) {
            g.refreshLoad(loadMapping);
        }
    }

    // Called by flush-tuple-timer thread
    public boolean publishFlushTuple() {
        if (receiveQueue.tryPublishDirect(flushTuple)) {
            LOG.debug("Published Flush tuple to: {} ", getComponentId());
            return true;
        } else {
            LOG.debug("RecvQ is currently full, will retry publishing Flush Tuple later to : {}", getComponentId());
            return false;
        }
    }

    /**
     * Returns map of stream id to component id to grouper.
     */
    private Map<String, Map<String, LoadAwareCustomStreamGrouping>> outboundComponents(
        WorkerTopologyContext workerTopologyContext, String componentId, Map<String, Object> topoConf) {
        Map<String, Map<String, LoadAwareCustomStreamGrouping>> ret = new HashMap<>();

        Map<String, Map<String, Grouping>> outputGroupings = workerTopologyContext.getTargets(componentId);
        for (Map.Entry<String, Map<String, Grouping>> entry : outputGroupings.entrySet()) {
            String streamId = entry.getKey();
            Map<String, Grouping> componentGrouping = entry.getValue();
            Fields outFields = workerTopologyContext.getComponentOutputFields(componentId, streamId);
            Map<String, LoadAwareCustomStreamGrouping> componentGrouper = new HashMap<String, LoadAwareCustomStreamGrouping>();
            for (Map.Entry<String, Grouping> cg : componentGrouping.entrySet()) {
                String component = cg.getKey();
                Grouping grouping = cg.getValue();
                List<Integer> outTasks = workerTopologyContext.getComponentTasks(component);
                LoadAwareCustomStreamGrouping grouper = GrouperFactory.mkGrouper(
                    workerTopologyContext, componentId, streamId, outFields, grouping, outTasks, topoConf);
                componentGrouper.put(component, grouper);
            }
            if (componentGrouper.size() > 0) {
                ret.put(streamId, componentGrouper);
            }
        }

        for (String stream : workerTopologyContext.getComponentCommon(componentId).get_streams().keySet()) {
            if (!ret.containsKey(stream)) {
                ret.put(stream, null);
            }
        }

        return ret;
    }

    // =============================================================================
    // ============================ getter methods =================================
    // =============================================================================

    private Map<String, Object> normalizedComponentConf(
        Map<String, Object> topoConf, WorkerTopologyContext topologyContext, String componentId) {
        List<String> keysToRemove = retrieveAllConfigKeys();
        keysToRemove.remove(Config.TOPOLOGY_DEBUG);
        keysToRemove.remove(Config.TOPOLOGY_MAX_SPOUT_PENDING);
        keysToRemove.remove(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
        keysToRemove.remove(Config.TOPOLOGY_TRANSACTIONAL_ID);
        keysToRemove.remove(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        keysToRemove.remove(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS);
        keysToRemove.remove(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY);
        keysToRemove.remove(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT);
        keysToRemove.remove(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS);
        keysToRemove.remove(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT);
        keysToRemove.remove(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS);
        keysToRemove.remove(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS);
        keysToRemove.remove(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME);
        keysToRemove.remove(Config.TOPOLOGY_STATE_PROVIDER);
        keysToRemove.remove(Config.TOPOLOGY_STATE_PROVIDER_CONFIG);
        keysToRemove.remove(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);

        Map<String, Object> componentConf;
        String specJsonConf = topologyContext.getComponentCommon(componentId).get_json_conf();
        if (specJsonConf != null) {
            try {
                componentConf = (Map<String, Object>) JSONValue.parseWithException(specJsonConf);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            for (Object p : keysToRemove) {
                componentConf.remove(p);
            }
        } else {
            componentConf = new HashMap<>();
        }

        Map<String, Object> ret = new HashMap<>();
        ret.putAll(topoConf);
        ret.putAll(componentConf);

        return ret;
    }

    public List<Long> getExecutorId() {
        return executorId;
    }

    public List<Integer> getTaskIds() {
        return taskIds;
    }

    public String getComponentId() {
        return componentId;
    }

    public AtomicBoolean getOpenOrPrepareWasCalled() {
        return openOrPrepareWasCalled;
    }

    public Map<String, Object> getTopoConf() {
        return topoConf;
    }

    public String getStormId() {
        return stormId;
    }

    public abstract CommonStats getStats();

    public String getType() {
        return type;
    }

    public Boolean getIsDebug() {
        return isDebug;
    }

    public ExecutorTransfer getExecutorTransfer() {
        return executorTransfer;
    }

    public IReportError getReportError() {
        return reportError;
    }

    public WorkerTopologyContext getWorkerTopologyContext() {
        return workerTopologyContext;
    }

    public boolean samplerCheck() {
        return sampler.getAsBoolean();
    }

    public AtomicReference<Map<String, DebugOptions>> getStormComponentDebug() {
        return stormComponentDebug;
    }

    public JCQueue getReceiveQueue() {
        return receiveQueue;
    }

    public IStormClusterState getStormClusterState() {
        return stormClusterState;
    }

    public WorkerState getWorkerData() {
        return workerData;
    }

    public Map<String, Map<String, LoadAwareCustomStreamGrouping>> getStreamToComponentToGrouper() {
        return streamToComponentToGrouper;
    }

    public HashMap getSharedExecutorData() {
        return sharedExecutorData;
    }

    public Map<Integer, Map<Integer, Map<String, IMetric>>> getIntervalToTaskToMetricToRegistry() {
        return intervalToTaskToMetricToRegistry;
    }

    @VisibleForTesting
    public void setLocalExecutorTransfer(ExecutorTransfer executorTransfer) {
        this.executorTransfer = executorTransfer;
    }

    public void incrementReportedErrorCount() {
        reportedErrorCount.inc(1L);
    }
}
