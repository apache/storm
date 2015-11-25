/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.daemon.nimbus;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.MetricMeta;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.DefaultMetricUploader;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.MetricUploader;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.codahale.metrics.Gauge;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Topology metrics thread which resides in nimbus.
 * This class is responsible for generating metrics IDs and uploading metrics to the underlying storage system.
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class TopologyMetricsRunnable extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyMetricsRunnable.class);

    protected JStormMetricCache metricCache;

    /**
     * map<topologyId, map<worker, metricInfo>>, local memory cache, keeps only one snapshot of metrics.
     */
    protected final ConcurrentMap<String, TopologyMetricContext> topologyMetricContexts =
            new ConcurrentHashMap<>();

    protected final BlockingDeque<TopologyMetricsRunnable.Event> queue = new LinkedBlockingDeque<>();

    private static final String PENDING_UPLOAD_METRIC_DATA = "__pending.upload.metrics__";
    private static final String PENDING_UPLOAD_METRIC_DATA_INFO = "__pending.upload.metrics.info__";

    // the slot is empty
    private static final int UNSET = 0;
    // the slot is ready for uploading
    private static final int SET = 1;
    // the slot is being uploaded
    private static final int UPLOADING = 2;
    // the slot will be set ready for uploading
    private static final int PRE_SET = 3;

    protected final AtomicIntegerArray metricStat;

    protected StormClusterState stormClusterState;

    protected MetricUploader metricUploader;

    protected AtomicBoolean isShutdown;
    protected String clusterName;
    protected int maxPendingUploadMetrics;

    private final boolean localMode;
    private final NimbusData nimbusData;
    private MetricQueryClient metricQueryClient;

    private ScheduledExecutorService clusterMetricsUpdateExecutor;

    /**
     * refreshes alive topologies every min or on startup.
     */
    protected AsyncLoopThread refreshTopologiesThread;

    /**
     * the thread for metric sending, checks every second.
     */
    private final Thread uploadThread = new MetricsUploadThread();

    /**
     * async flush metric meta
     */
    private final Thread flushMetricMetaThread = new FlushMetricMetaThread();

    /**
     * use default UUID generator
     */
    private final MetricIDGenerator metricIDGenerator = new DefaultMetricIDGenerator();

    public TopologyMetricsRunnable(final NimbusData nimbusData) {
        setName(getClass().getSimpleName());

        this.nimbusData = nimbusData;

        this.localMode = nimbusData.isLocalMode();
        if (localMode) {
            this.metricStat = new AtomicIntegerArray(1);
            return;
        }

        LOG.info("create topology metrics runnable.");
        this.metricCache = nimbusData.getMetricCache();
        this.stormClusterState = nimbusData.getStormClusterState();
        this.isShutdown = nimbusData.getIsShutdown();

        clusterName = ConfigExtension.getClusterName(nimbusData.getConf());
        if (clusterName == null) {
            throw new RuntimeException("cluster.name property must be set in storm.yaml!");
        }

        this.maxPendingUploadMetrics = ConfigExtension.getMaxPendingMetricNum(nimbusData.getConf());
        this.metricStat = new AtomicIntegerArray(this.maxPendingUploadMetrics);

        int cnt = 0;
        for (int i = 0; i < maxPendingUploadMetrics; i++) {
            TopologyMetricDataInfo obj = getMetricDataInfoFromCache(i);
            if (obj != null) {
                this.metricStat.set(i, SET);
                cnt++;
            }
        }
        LOG.info("pending upload metrics: {}", cnt);

        // init alive topologies from zk
        this.refreshTopologies();
        this.refreshTopologiesThread = new AsyncLoopThread(new RefreshTopologiesThread());

        this.clusterMetricsUpdateExecutor = Executors.newSingleThreadScheduledExecutor();
        this.clusterMetricsUpdateExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                int secOffset = TimeUtils.secOffset();
                int offset = 55;
                if (secOffset < offset) {
                    JStormUtils.sleepMs((offset - secOffset) * 1000);
                } else if (secOffset == offset) {
                    // do nothing
                } else {
                    JStormUtils.sleepMs((60 - secOffset + offset) * 1000);
                }

                LOG.info("cluster metrics force upload.");
                mergeAndUploadClusterMetrics();
            }
        }, 5, 60, TimeUnit.SECONDS);

        // track nimbus JVM heap
        JStormMetrics.registerWorkerGauge(JStormMetrics.NIMBUS_METRIC_KEY, MetricDef.MEMORY_USED,
                new AsmGauge(new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
                        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
                        return (double) memoryUsage.getUsed();
                    }
                }));
    }

    /**
     * init metric uploader
     */
    public void init() {
        String metricUploadClass = ConfigExtension.getMetricUploaderClass(nimbusData.getConf());
        if (StringUtils.isBlank(metricUploadClass)) {
            metricUploadClass = DefaultMetricUploader.class.getName();
        }
        // init metric uploader
        LOG.info("metric uploader class:{}", metricUploadClass);
        Object instance = Utils.newInstance(metricUploadClass);
        if (!(instance instanceof MetricUploader)) {
            throw new RuntimeException(metricUploadClass + " isn't MetricUploader class ");
        }
        this.metricUploader = (MetricUploader) instance;
        try {
            metricUploader.init(nimbusData);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("Successfully init {}", metricUploadClass);

        // init metric query client
        String metricQueryClientClass = ConfigExtension.getMetricQueryClientClass(nimbusData.getConf());
        if (!StringUtils.isBlank(metricQueryClientClass)) {
            LOG.info("metric query client class:{}", metricQueryClientClass);
            this.metricQueryClient = (MetricQueryClient) Utils.newInstance(metricQueryClientClass);
        } else {
            LOG.warn("use default metric query client class.");
            this.metricQueryClient = new DefaultMetricQueryClient();
        }
        try {
            metricQueryClient.init(nimbusData.getConf());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.uploadThread.start();
        this.flushMetricMetaThread.start();

        LOG.info("init topology metric runnable done.");
    }

    public void shutdown() {
        LOG.info("Begin to shutdown");
        metricUploader.cleanup();

        LOG.info("Successfully shutdown");
    }

    @Override
    public void run() {
        while (!isShutdown.get()) {
            if (localMode) {
                return;
            }

            try {
                // wait for metricUploader to be ready, for some external plugin like database, it'll take a few seconds
                if (this.metricUploader != null) {
                    Event event = queue.poll();
                    if (event == null) {
                        continue;
                    }

                    if (event instanceof Remove) {
                        handleRemoveEvent((Remove) event);
                    } else if (event instanceof Update) {
                        handleUpdateEvent((Update) event);
                    } else if (event instanceof Refresh) {
                        handleRefreshEvent((Refresh) event);
                    } else if (event instanceof KillTopologyEvent) {
                        handleKillTopologyEvent((KillTopologyEvent) event);
                    } else if (event instanceof StartTopologyEvent) {
                        handleStartTopologyEvent((StartTopologyEvent) event);
                    } else if (event instanceof TaskDeadEvent) {
                        handleTaskDeadEvent((TaskDeadEvent) event);
                    } else if (event instanceof TaskStartEvent) {
                        handleTaskStartEvent((TaskStartEvent) event);
                    } else {
                        LOG.error("Unknown event type:{}", event.getClass());
                    }
                }
            } catch (Exception e) {
                if (!isShutdown.get()) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }


    public boolean isTopologyAlive(String topologyId) {
        return topologyMetricContexts.containsKey(topologyId);
    }

    private int getAndPresetFirstEmptyIndex() {
        for (int i = 0; i < maxPendingUploadMetrics; i++) {
            if (metricStat.get(i) == UNSET) {
                if (metricStat.compareAndSet(i, UNSET, PRE_SET)) {
                    return i;
                }
            }
        }
        return -1;
    }

    private int getFirstPendingUploadIndex() {
        for (int i = 0; i < maxPendingUploadMetrics; i++) {
            if (metricStat.get(i) == SET) {
                return i;
            }
        }
        return -1;
    }

    public void markUploaded(int idx) {
        this.metricCache.remove(PENDING_UPLOAD_METRIC_DATA + idx);
        this.metricCache.remove(PENDING_UPLOAD_METRIC_DATA_INFO + idx);
        this.metricStat.set(idx, UNSET);
    }

    public void markUploading(int idx) {
        this.metricStat.set(idx, UPLOADING);
    }

    public void markSet(int idx) {
        this.metricStat.set(idx, SET);
    }

    public TopologyMetric getMetricDataFromCache(int idx) {
        return (TopologyMetric) metricCache.get(PENDING_UPLOAD_METRIC_DATA + idx);
    }

    public TopologyMetricDataInfo getMetricDataInfoFromCache(int idx) {
        return (TopologyMetricDataInfo) metricCache.get(PENDING_UPLOAD_METRIC_DATA_INFO + idx);
    }

    public void pushEvent(Event cmd) {
        queue.offer(cmd);
    }

    public Map<String, Long> registerMetrics(String topologyId, Set<String> metricNames) {
        TimeTicker ticker = new TimeTicker(TimeUnit.MILLISECONDS, true);

        ConcurrentMap<String, Long> memMeta = topologyMetricContexts.get(topologyId).getMemMeta();
        Map<String, Long> ret = new HashMap<>();
        for (String metricName : metricNames) {
            Long id = memMeta.get(metricName);
            if (id != null && MetricUtils.isValidId(id)) {
                ret.put(metricName, id);
            } else {
                id = metricIDGenerator.genMetricId(metricName);
                Long old = memMeta.putIfAbsent(metricName, id);
                if (old == null) {
                    ret.put(metricName, id);
                } else {
                    ret.put(metricName, old);
                }
            }
        }

        long cost = ticker.stop();
        LOG.info("register metrics, topology:{}, size:{}, cost:{}", topologyId, metricNames.size(), cost);

        return ret;
    }

    public void handleRemoveEvent(Remove event) {
        String topologyId = event.topologyId;
        if (topologyId != null) {
            removeTopology(topologyId);
        }
        LOG.info("remove topology:{}.", topologyId);

    }

    private void removeTopology(String topologyId) {
        metricCache.removeTopology(topologyId);
        metricCache.removeSampleRate(topologyId);

        topologyMetricContexts.remove(topologyId);
    }


    public void refreshTopologies() {
        if (!topologyMetricContexts.containsKey(JStormMetrics.NIMBUS_METRIC_KEY)) {
            LOG.info("adding __nimbus__ to metric context.");
            Set<ResourceWorkerSlot> workerSlot = Sets.newHashSet(new ResourceWorkerSlot());
            TopologyMetricContext metricContext = new TopologyMetricContext(workerSlot);
            topologyMetricContexts.putIfAbsent(JStormMetrics.NIMBUS_METRIC_KEY, metricContext);
            syncMetaFromCache(JStormMetrics.NIMBUS_METRIC_KEY, topologyMetricContexts.get(JStormMetrics.NIMBUS_METRIC_KEY));
        }
        if (!topologyMetricContexts.containsKey(JStormMetrics.CLUSTER_METRIC_KEY)) {
            LOG.info("adding __cluster__ to metric context.");
            Set<ResourceWorkerSlot> workerSlot = Sets.newHashSet(new ResourceWorkerSlot());
            Map conf = new HashMap();
            //there's no need to consider sample rate when cluster metrics merge
            conf.put(ConfigExtension.TOPOLOGY_METRIC_SAMPLE_RATE, 1.0);
            TopologyMetricContext metricContext = new TopologyMetricContext(
                    JStormMetrics.CLUSTER_METRIC_KEY, workerSlot, conf);
            topologyMetricContexts.putIfAbsent(JStormMetrics.CLUSTER_METRIC_KEY, metricContext);
            syncMetaFromCache(JStormMetrics.CLUSTER_METRIC_KEY, topologyMetricContexts.get(JStormMetrics.CLUSTER_METRIC_KEY));
        }

        Map<String, Assignment> assignMap;
        try {
            assignMap = Cluster.get_all_assignment(stormClusterState, null);
            for (String topologyId : assignMap.keySet()) {
                if (!topologyMetricContexts.containsKey(topologyId)) {
                    Assignment assignment = assignMap.get(topologyId);
                    TopologyMetricContext metricContext =
                            new TopologyMetricContext(assignment.getWorkers());
                    metricContext.setTaskNum(NimbusUtils.getTopologyTaskNum(assignment));
                    syncMetaFromCache(topologyId, metricContext);

                    LOG.info("adding {} to metric context.", topologyId);
                    topologyMetricContexts.put(topologyId, metricContext);
                }
            }
        } catch (Exception e1) {
            LOG.warn("failed to get assignments");
            return;
        }

        List<String> removing = new ArrayList<>();
        for (String topologyId : topologyMetricContexts.keySet()) {
            if (!JStormMetrics.NIMBUS_METRIC_KEY.equals(topologyId)
                    && !JStormMetrics.CLUSTER_METRIC_KEY.equals(topologyId)
                    && !assignMap.containsKey(topologyId)) {
                removing.add(topologyId);
            }
        }

        for (String topologyId : removing) {
            LOG.info("removing topology:{}", topologyId);
            removeTopology(topologyId);
        }
    }

    /**
     * sync topology metric meta from external storage like TDDL/OTS.
     * nimbus server will skip syncing, only followers do this
     */
    public void syncTopologyMeta() {
        String nimbus = JStormMetrics.NIMBUS_METRIC_KEY;
        if (topologyMetricContexts.containsKey(nimbus)) {
            syncMetaFromRemote(nimbus, topologyMetricContexts.get(nimbus));
        }
        String cluster = JStormMetrics.CLUSTER_METRIC_KEY;
        if (topologyMetricContexts.containsKey(cluster)) {
            syncMetaFromRemote(cluster, topologyMetricContexts.get(cluster));
        }

        Map<String, Assignment> assignMap;
        try {
            assignMap = Cluster.get_all_assignment(stormClusterState, null);
            for (String topologyId : assignMap.keySet()) {
                if (topologyMetricContexts.containsKey(topologyId)) {
                    Assignment assignment = assignMap.get(topologyId);
                    TopologyMetricContext metricContext =
                            new TopologyMetricContext(assignment.getWorkers());
                    metricContext.setTaskNum(NimbusUtils.getTopologyTaskNum(assignment));

                    syncMetaFromCache(topologyId, metricContext);
                    syncMetaFromRemote(topologyId, metricContext);
                }
            }
        } catch (Exception e1) {
            LOG.warn("failed to get assignments");
        }
    }

    /**
     * sync metric meta from rocks db into mem cache on startup
     */
    private void syncMetaFromCache(String topologyId, TopologyMetricContext context) {
        if (!context.syncMeta()) {
            Map<String, Long> meta = metricCache.getMeta(topologyId);
            if (meta != null) {
                context.getMemMeta().putAll(meta);
            }
            context.setSyncMeta(true);
        }
    }

    private void syncMetaFromRemote(String topologyId, TopologyMetricContext context) {
        try {
            int memSize = context.getMemMeta().size();
            int zkSize = (Integer) stormClusterState.get_topology_metric(topologyId);

            if (memSize != zkSize) {
                ConcurrentMap<String, Long> memMeta = context.getMemMeta();
                for (MetaType metaType : MetaType.values()) {
                    List<MetricMeta> metaList = metricQueryClient.getMetricMeta(clusterName, topologyId, metaType);
                    if (metaList != null) {
                        LOG.info("get remote metric meta, topology:{}, metaType:{}, mem:{}, zk:{}, new size:{}",
                                topologyId, metaType, memSize, zkSize, metaList.size());
                        for (MetricMeta meta : metaList) {
                            memMeta.putIfAbsent(meta.getFQN(), meta.getId());
                        }
                    }
                }
                metricCache.putMeta(topologyId, memMeta);
            }
        } catch (Exception ex) {
            LOG.error("failed to sync remote meta", ex);
        }
    }

    /**
     * send topology track to jstorm monitor
     */
    protected void handleKillTopologyEvent(KillTopologyEvent event) {
        metricUploader.sendEvent(this.clusterName, event);
        removeTopology(event.topologyId);
    }

    private void handleStartTopologyEvent(StartTopologyEvent event) {
        this.metricCache.putSampleRate(event.topologyId, event.sampleRate);
        metricUploader.sendEvent(this.clusterName, event);
        if (!topologyMetricContexts.containsKey(event.topologyId)) {
            TopologyMetricContext metricContext = new TopologyMetricContext();
            // note that workerNum is not set here.
            this.topologyMetricContexts.put(event.topologyId, metricContext);
        }
    }

    private void handleTaskDeadEvent(TaskDeadEvent event) {
        metricUploader.sendEvent(this.clusterName, event);

        // unregister dead workers
        Set<ResourceWorkerSlot> workers = new HashSet<>();
        workers.addAll(event.deadTasks.values());
        for (ResourceWorkerSlot worker : workers) {
            metricCache.unregisterWorker(event.topologyId, worker.getHostname(), worker.getPort());
        }
    }

    private void handleTaskStartEvent(final TaskStartEvent event) {
        Assignment assignment = event.newAssignment;
        TopologyMetricContext metricContext = topologyMetricContexts.get(event.topologyId);
        if (metricContext != null) {
            metricContext.setWorkerSet(assignment.getWorkers());
        } else {
            metricContext = new TopologyMetricContext();
            metricContext.setWorkerSet(assignment.getWorkers());
            topologyMetricContexts.put(event.topologyId, metricContext);
        }
        metricUploader.sendEvent(this.clusterName, event);
    }

    /**
     * merge and send all metric data.
     */
    public void handleRefreshEvent(Refresh dummy) {
        TimeTicker ticker = new TimeTicker(TimeUnit.MILLISECONDS, true);
        try {
            refreshTopologies();
            LOG.info("refresh topologies, cost:{}", ticker.stopAndRestart());
            if (!nimbusData.isLeader()) {
                syncTopologyMeta();
                LOG.info("sync topology meta, cost:{}", ticker.stop());
            }
        } catch (Exception ex) {
            LOG.error("handleRefreshEvent error:", ex);
        }
    }

    private TopologyMetricContext getClusterTopologyMetricContext() {
        return topologyMetricContexts.get(JStormMetrics.CLUSTER_METRIC_KEY);
    }

    private void mergeAndUploadClusterMetrics() {
        TopologyMetricContext context = getClusterTopologyMetricContext();
        TopologyMetric tpMetric = context.mergeMetrics();
        if (tpMetric == null) {
            tpMetric = MetricUtils.mkTopologyMetric();
            tpMetric.set_topologyMetric(MetricUtils.mkMetricInfo());
        }

        //reset snapshots metric id
        MetricInfo clusterMetrics = tpMetric.get_topologyMetric();
        Map<String, Long> metricNames = context.getMemMeta();
        for (Map.Entry<String, Map<Integer, MetricSnapshot>> entry : clusterMetrics.get_metrics().entrySet()) {
            String metricName = entry.getKey();
            MetricType metricType = MetricUtils.metricType(metricName);
            Long metricId = metricNames.get(metricName);
            for (Map.Entry<Integer, MetricSnapshot> metric : entry.getValue().entrySet()) {
                MetricSnapshot snapshot = metric.getValue();
                snapshot.set_metricId(metricId);
                if (metricType == MetricType.HISTOGRAM || metricType == MetricType.TIMER) {
                    snapshot.set_points(new ArrayList<Long>(0));
                }
//                entry.getValue().put(metric.getKey(), snapshot);
            }
        }

        //fill the unacquired metrics with zero
        long ts = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : metricNames.entrySet()) {
            String name = entry.getKey();
            if (!clusterMetrics.get_metrics().containsKey(name)) {
                Map<Integer, MetricSnapshot> metric = new HashMap<>();
                MetricType type = MetricUtils.metricType(name);
                metric.put(AsmWindow.M1_WINDOW, new MetricSnapshot(entry.getValue(), ts, type.getT()));
                clusterMetrics.put_to_metrics(name, metric);
            }
        }

        //upload to cache
        Update event = new Update();
        event.timestamp = System.currentTimeMillis();
        event.topologyMetrics = tpMetric;
        event.topologyId = JStormMetrics.CLUSTER_METRIC_KEY;
        pushEvent(event);

        LOG.info("send update event for cluster metrics, size : {}", clusterMetrics.get_metrics_size());
    }

    //update cluster metrics local cache
    private void updateClusterMetrics(String topologyId, TopologyMetric tpMetric) {
        if (tpMetric.get_topologyMetric().get_metrics_size() > 0) {
            TopologyMetricContext context = getClusterTopologyMetricContext();
            MetricInfo topologyMetrics = tpMetric.get_topologyMetric();
            // make a new MetricInfo to save the topologyId's metric
            MetricInfo clusterMetrics = MetricUtils.mkMetricInfo();
            Set<String> metricNames = new HashSet<>();
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> entry : topologyMetrics.get_metrics().entrySet()) {
                String metricName = MetricUtils.topo2clusterName(entry.getKey());
                MetricType metricType = MetricUtils.metricType(metricName);
                Map<Integer, MetricSnapshot> winData = new HashMap<>();
                for (Map.Entry<Integer, MetricSnapshot> entryData : entry.getValue().entrySet()) {
                    MetricSnapshot snapshot = entryData.getValue().deepCopy();
                    winData.put(entryData.getKey(), snapshot);
                    if (metricType == MetricType.HISTOGRAM || metricType == MetricType.TIMER) {
                        // reset topology metric points
                        entryData.getValue().set_points(new ArrayList<Long>(0));
                    }
                }
                clusterMetrics.put_to_metrics(metricName, winData);
                metricNames.add(metricName);
            }
            // save to local cache, waiting for merging
            context.addToMemCache(topologyId, clusterMetrics);
            registerMetrics(JStormMetrics.CLUSTER_METRIC_KEY, metricNames);
        }
    }

    /**
     * put metric data to metric cache.
     */
    public void handleUpdateEvent(Update event) {
        TopologyMetric topologyMetrics = event.topologyMetrics;
        final String topologyId = event.topologyId;

        if (this.topologyMetricContexts.containsKey(topologyId)) {
            if (!JStormMetrics.CLUSTER_METRIC_KEY.equals(topologyId)) {
                updateClusterMetrics(topologyId, topologyMetrics);
            }

            // overwrite
            metricCache.putMetricData(topologyId, topologyMetrics);

            // below process is kind of a transaction, first we lock an empty slot, mark it as PRE_SET
            // by this time the slot is not yet ready for uploading as the upload thread looks for SET slots only
            // after all metrics data has been saved, we mark it as SET, then it's ready for uploading.
            int idx = getAndPresetFirstEmptyIndex();
            if (idx >= 0) {
                TopologyMetricDataInfo summary = new TopologyMetricDataInfo();
                summary.topologyId = topologyId;
                summary.timestamp = event.timestamp;
                if (topologyId.equals(JStormMetrics.NIMBUS_METRIC_KEY) ||
                        topologyId.equals(JStormMetrics.CLUSTER_METRIC_KEY)) {
                    summary.type = MetricUploader.METRIC_TYPE_TOPLOGY;
                } else {
                    if (topologyMetrics.get_topologyMetric().get_metrics_size() > 0 ||
                            topologyMetrics.get_componentMetric().get_metrics_size() > 0) {
                        if (topologyMetrics.get_taskMetric().get_metrics_size() +
                                topologyMetrics.get_workerMetric().get_metrics_size() +
                                topologyMetrics.get_nettyMetric().get_metrics_size() +
                                topologyMetrics.get_streamMetric().get_metrics_size() > 0) {
                            summary.type = MetricUploader.METRIC_TYPE_ALL;
                        } else {
                            summary.type = MetricUploader.METRIC_TYPE_TOPLOGY;
                        }
                    } else {
                        summary.type = MetricUploader.METRIC_TYPE_TASK;
                    }
                }

                metricCache.put(PENDING_UPLOAD_METRIC_DATA_INFO + idx, summary);
                metricCache.put(PENDING_UPLOAD_METRIC_DATA + idx, topologyMetrics);
                markSet(idx);
                LOG.info("put metric data to local cache, topology:{}, idx:{}", topologyId, idx);
            } else {
                LOG.error("exceeding maxPendingUploadMetrics, skip metrics data for topology:{}", topologyId);
            }
        } else {
            LOG.warn("topology {} has been killed or has not started, skip update.", topologyId);
        }
    }

    /**
     * get topology metrics, note that only topology & component & worker metrics are returned
     */
    public TopologyMetric getTopologyMetric(String topologyId) {
        long start = System.nanoTime();
        try {
            TopologyMetric ret = new TopologyMetric();
            List<MetricInfo> topologyMetrics = metricCache.getMetricData(topologyId, MetaType.TOPOLOGY);
            List<MetricInfo> componentMetrics = metricCache.getMetricData(topologyId, MetaType.COMPONENT);
            List<MetricInfo> workerMetrics = metricCache.getMetricData(topologyId, MetaType.WORKER);

            MetricInfo dummy = MetricUtils.mkMetricInfo();
            if (topologyMetrics.size() > 0) {
                // get the last min topology metric
                ret.set_topologyMetric(topologyMetrics.get(topologyMetrics.size() - 1));
            } else {
                ret.set_topologyMetric(dummy);
            }
            if (componentMetrics.size() > 0) {
                ret.set_componentMetric(componentMetrics.get(0));
            } else {
                ret.set_componentMetric(dummy);
            }
            if (workerMetrics.size() > 0) {
                ret.set_workerMetric(workerMetrics.get(0));
            } else {
                ret.set_workerMetric(dummy);
            }
            ret.set_taskMetric(dummy);
            ret.set_streamMetric(dummy);
            ret.set_nettyMetric(dummy);

            return ret;
        } finally {
            long end = System.nanoTime();
            SimpleJStormMetric.updateNimbusHistogram("getTopologyMetric", (end - start) / TimeUtils.NS_PER_US);
        }
    }

    public static String getWorkerSlotName(String hostname, Integer port) {
        return hostname + ":" + port;
    }

    class RefreshTopologiesThread extends RunnableCallback {
        @Override
        public void run() {
            if (!isShutdown.get()) {
                pushEvent(new Refresh());
            }
        }

        @Override
        public Object getResult() {
            return TimeUtils.SEC_PER_MIN;
        }

        @Override
        public String getThreadName() {
            return "RefreshThread";
        }
    }

    class MetricsUploadThread extends Thread {
        public MetricsUploadThread() {
            setName("main-upload-thread");
        }

        @Override
        public void run() {
            while (!isShutdown.get()) {
                try {
                    if (metricUploader != null && nimbusData.isLeader()) {
                        final int idx = getFirstPendingUploadIndex();
                        if (idx >= 0) {
                            markUploading(idx);
                            upload(clusterName, idx);
                        }
                    }
                    JStormUtils.sleepMs(5);
                } catch (Exception ex) {
                    LOG.error("Error", ex);
                }
            }
        }

        public boolean upload(final String clusterName, final int idx) {
            final TopologyMetricDataInfo summary = getMetricDataInfoFromCache(idx);
            if (summary == null) {
                LOG.warn("metric summary is null from cache idx:{}", idx);
                markUploaded(idx);
                return true;
            }

            final String topologyId = summary.topologyId;
            if (!isTopologyAlive(topologyId)) {
                LOG.warn("topology {} is not alive, skip sending metrics.", topologyId);
                markUploaded(idx);
                return true;
            }

            return metricUploader.upload(clusterName, topologyId, idx, summary.toMap());
        }
    }

    class FlushMetricMetaThread extends Thread {

        public FlushMetricMetaThread() {
            setName("FlushMetricMetaThread");
        }

        @Override
        public void run() {
            while (!isShutdown.get()) {
                long start = System.currentTimeMillis();
                try {
                    // if metricUploader is not fully initialized, return directly
                    if (nimbusData.isLeader() && metricUploader != null) {
                        for (Map.Entry<String, TopologyMetricContext> entry : topologyMetricContexts.entrySet()) {
                            String topologyId = entry.getKey();
                            TopologyMetricContext metricContext = entry.getValue();

                            Map<String, Long> cachedMeta = metricCache.getMeta(topologyId);
                            if (cachedMeta == null) {
                                cachedMeta = new HashMap<>();
                            }
                            Map<String, Long> memMeta = metricContext.getMemMeta();
                            if (memMeta.size() > cachedMeta.size()) {
                                cachedMeta.putAll(memMeta);
                            }
                            metricCache.putMeta(topologyId, cachedMeta);

                            int curSize = cachedMeta.size();
                            if (curSize != metricContext.getFlushedMetaNum()) {
                                metricContext.setFlushedMetaNum(curSize);

                                metricUploader.registerMetrics(clusterName, topologyId, cachedMeta);
                                LOG.info("flush metric meta, topology:{}, total:{}, cost:{}.",
                                        topologyId, curSize, System.currentTimeMillis() - start);
                            }
                            stormClusterState.set_topology_metric(topologyId, curSize);
                        }
                    }

                    JStormUtils.sleepMs(15000);
                } catch (Exception ex) {
                    LOG.error("Error", ex);
                }
            }
        }
    }

    public static class TopologyMetricDataInfo implements Serializable {
        private static final long serialVersionUID = 1303262512351757610L;

        public String topologyId;
        public String type; // "tp" for tp/comp metrics OR "task" for task/stream/worker/netty metrics
        public long timestamp;   // metrics report time

        public Map<String, Object> toMap() {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(MetricUploader.METRIC_TIME, timestamp);
            ret.put(MetricUploader.METRIC_TYPE, type);

            return ret;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

    // ==============================================
    // =================== events ===================
    // ==============================================
    public static class Event {
        protected Event() {
        }

        public String clusterName;
        public String topologyId;
        public long timestamp;

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

    public static class Update extends Event {
        public TopologyMetric topologyMetrics;
    }

    public static class Remove extends Event {
    }

    public static class Refresh extends Event {
    }


    public static class KillTopologyEvent extends Event {
    }

    public static class StartTopologyEvent extends Event {
        public double sampleRate;
    }

    public static class TaskDeadEvent extends Event {
        public Map<Integer, ResourceWorkerSlot> deadTasks;
    }

    public static class TaskStartEvent extends Event {
        public Assignment oldAssignment;
        public Assignment newAssignment;
        public Map<Integer, String> task2Component;
    }
}
