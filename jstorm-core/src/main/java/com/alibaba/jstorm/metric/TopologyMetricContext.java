package com.alibaba.jstorm.metric;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.TopologyMetric;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A topology metric context contains all in-memory metric data of a topology.
 * This class resides in TopologyMaster.
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class TopologyMetricContext {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final ReentrantLock lock = new ReentrantLock();
    private Set<ResourceWorkerSlot> workerSet;
    private int taskNum = 1;
    private ConcurrentMap<String, MetricInfo> memCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> memMeta = new ConcurrentHashMap<>();
    private final AtomicBoolean isMerging = new AtomicBoolean(false);
    private String topologyId;
    private volatile int flushedMetaNum = 0;

    /**
     * sync meta from metric cache on startup
     */
    private volatile boolean syncMeta = false;

    private Map conf;

    public TopologyMetricContext() {
    }

    public TopologyMetricContext(Set<ResourceWorkerSlot> workerSet) {
        this.workerSet = workerSet;
    }

    public TopologyMetricContext(String topologyId, Set<ResourceWorkerSlot> workerSet, Map conf) {
        this(workerSet);
        this.topologyId = topologyId;
        this.conf = conf;
    }

    public ConcurrentMap<String, Long> getMemMeta() {
        return memMeta;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }

    public boolean syncMeta() {
        return syncMeta;
    }

    public void setSyncMeta(boolean syncMeta) {
        this.syncMeta = syncMeta;
    }

    public int getTaskNum() {
        return taskNum;
    }

    public void setTaskNum(int taskNum) {
        this.taskNum = taskNum;
    }

    public int getFlushedMetaNum() {
        return flushedMetaNum;
    }

    public void setFlushedMetaNum(int flushedMetaNum) {
        this.flushedMetaNum = flushedMetaNum;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public int getWorkerNum() {
        return workerSet.size();
    }

    public void setWorkerSet(Set<ResourceWorkerSlot> workerSet) {
        this.workerSet = workerSet;
    }

    public void resetUploadedMetrics() {
        this.memCache.clear();
    }

    public final ConcurrentMap<String, MetricInfo> getMemCache() {
        return memCache;
    }

    public void addToMemCache(String workerSlot, MetricInfo metricInfo) {
        memCache.put(workerSlot, metricInfo);
        LOG.info("update mem cache, worker:{}, total uploaded:{}", workerSlot, memCache.size());
    }

    public boolean readyToUpload() {
        return memCache.size() >= workerSet.size();
    }

    public boolean isMerging() {
        return isMerging.get();
    }

    public void setMerging(boolean isMerging) {
        this.isMerging.set(isMerging);
    }

    public int getUploadedWorkerNum() {
        return memCache.size();
    }

    public TopologyMetric mergeMetrics() {
        long start = System.currentTimeMillis();

        if (getMemCache().size() == 0) {
            //LOG.info("topology:{}, metric size is 0, skip...", topologyId);
            return null;
        }
        if (isMerging()) {
            LOG.info("topology {} is already merging, skip...", topologyId);
            return null;
        }

        setMerging(true);

        try {
            Map<String, MetricInfo> workerMetricMap = this.memCache;
            // reset mem cache
            this.memCache = new ConcurrentHashMap<>();

            MetricInfo topologyMetrics = MetricUtils.mkMetricInfo();
            MetricInfo componentMetrics = MetricUtils.mkMetricInfo();
            MetricInfo taskMetrics = MetricUtils.mkMetricInfo();
            MetricInfo streamMetrics = MetricUtils.mkMetricInfo();
            MetricInfo workerMetrics = MetricUtils.mkMetricInfo();
            MetricInfo nettyMetrics = MetricUtils.mkMetricInfo();
            TopologyMetric tpMetric =
                    new TopologyMetric(topologyMetrics, componentMetrics, workerMetrics, taskMetrics, streamMetrics, nettyMetrics);


            // metric name => worker count
            Map<String, Integer> metricNameCounters = new HashMap<>();

            // special for histograms & timers, we merge the points to get a new snapshot data.
            Map<String, Map<Integer, Histogram>> histograms = new HashMap<>();
            Map<String, Map<Integer, Timer>> timers = new HashMap<>();

            // iterate metrics of all workers within the same topology
            for (ConcurrentMap.Entry<String, MetricInfo> metricEntry : workerMetricMap.entrySet()) {
                MetricInfo metricInfo = metricEntry.getValue();

                // merge counters: add old and new values, note we only add incoming new metrics and overwrite
                // existing data, same for all below.
                Map<String, Map<Integer, MetricSnapshot>> metrics = metricInfo.get_metrics();
                for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : metrics.entrySet()) {
                    String metricName = metric.getKey();
                    Map<Integer, MetricSnapshot> data = metric.getValue();
                    MetaType metaType = MetricUtils.metaType(metricName);

                    MetricType metricType = MetricUtils.metricType(metricName);
                    if (metricType == MetricType.COUNTER) {
                        mergeCounters(tpMetric, metaType, metricName, data);
                    } else if (metricType == MetricType.GAUGE) {
                        mergeGauges(tpMetric, metaType, metricName, data);
                    } else if (metricType == MetricType.METER) {
                        mergeMeters(getMetricInfoByType(tpMetric, metaType), metricName, data, metricNameCounters);
                    } else if (metricType == MetricType.HISTOGRAM) {
                        mergeHistograms(getMetricInfoByType(tpMetric, metaType),
                                metricName, data, metricNameCounters, histograms);
                    } else if (metricType == MetricType.TIMER) {
                        mergeTimers(getMetricInfoByType(tpMetric, metaType),
                                metricName, data, metricNameCounters, timers);
                    }
                }
            }
            adjustHistogramTimerMetrics(tpMetric, metricNameCounters, histograms, timers);
            // for counters, we only report delta data every time, need to sum with old data
            //adjustCounterMetrics(tpMetric, oldTpMetric);

            LOG.info("merge topology metrics:{}, cost:{}", topologyId, System.currentTimeMillis() - start);
            // debug logs
            //MetricUtils.printMetricWinSize(componentMetrics);

            return tpMetric;
        } finally {
            setMerging(false);
        }
    }


    protected MetricInfo getMetricInfoByType(TopologyMetric topologyMetric, MetaType type) {
        if (type == MetaType.TASK) {
            return topologyMetric.get_taskMetric();
        } else if (type == MetaType.WORKER) {
            return topologyMetric.get_workerMetric();
        } else if (type == MetaType.COMPONENT) {
            return topologyMetric.get_componentMetric();
        } else if (type == MetaType.STREAM) {
            return topologyMetric.get_streamMetric();
        } else if (type == MetaType.NETTY) {
            return topologyMetric.get_nettyMetric();
        } else if (type == MetaType.TOPOLOGY) {
            return topologyMetric.get_topologyMetric();
        }
        return null;
    }

    public void mergeCounters(TopologyMetric tpMetric, MetaType metaType, String meta,
                              Map<Integer, MetricSnapshot> data) {
        MetricInfo metricInfo = getMetricInfoByType(tpMetric, metaType);
        Map<Integer, MetricSnapshot> existing = metricInfo.get_metrics().get(meta);
        if (existing == null) {
            metricInfo.put_to_metrics(meta, data);
        } else {
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Integer win = dataEntry.getKey();
                MetricSnapshot snapshot = dataEntry.getValue();
                MetricSnapshot old = existing.get(win);
                if (old == null) {
                    existing.put(win, snapshot);
                } else {
                    old.set_ts(snapshot.get_ts());
                    old.set_longValue(old.get_longValue() + snapshot.get_longValue());
                }
            }
        }
    }

    public void mergeGauges(TopologyMetric tpMetric, MetaType metaType, String meta,
                            Map<Integer, MetricSnapshot> data) {
        MetricInfo metricInfo = getMetricInfoByType(tpMetric, metaType);
        Map<Integer, MetricSnapshot> existing = metricInfo.get_metrics().get(meta);
        if (existing == null) {
            metricInfo.put_to_metrics(meta, data);
        } else {
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Integer win = dataEntry.getKey();
                MetricSnapshot snapshot = dataEntry.getValue();
                MetricSnapshot old = existing.get(win);
                if (old == null) {
                    existing.put(win, snapshot);
                } else {
                    if (snapshot.get_ts() >= old.get_ts()) {
                        old.set_ts(snapshot.get_ts());
                        if (metaType != MetaType.TOPOLOGY) {
                            old.set_doubleValue(snapshot.get_doubleValue());
                        } else { // for topology metric, gauge might be add-able, e.g., cpu, memory, etc.
                            old.set_doubleValue(old.get_doubleValue() + snapshot.get_doubleValue());
                        }
                    }
                }
            }
        }
    }

    /**
     * meters are not sampled.
     */
    public void mergeMeters(MetricInfo metricInfo, String meta, Map<Integer, MetricSnapshot> data,
                            Map<String, Integer> metaCounters) {
        Map<Integer, MetricSnapshot> existing = metricInfo.get_metrics().get(meta);
        if (existing == null) {
            metricInfo.put_to_metrics(meta, data);
        } else {
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Integer win = dataEntry.getKey();
                MetricSnapshot snapshot = dataEntry.getValue();
                MetricSnapshot old = existing.get(win);
                if (old == null) {
                    existing.put(win, snapshot);
                } else {
                    if (snapshot.get_ts() >= old.get_ts()) {
                        old.set_ts(snapshot.get_ts());
                        old.set_mean(old.get_mean() + snapshot.get_mean());
                        old.set_m1(old.get_m1() + snapshot.get_m1());
                        old.set_m5(old.get_m5() + snapshot.get_m5());
                        old.set_m15(old.get_m15() + snapshot.get_m15());
                    }
                }
            }
        }
        updateMetricCounters(meta, metaCounters);
    }

    /**
     * histograms are sampled, but we just update points
     */
    public void mergeHistograms(MetricInfo metricInfo, String meta, Map<Integer, MetricSnapshot> data,
                                Map<String, Integer> metaCounters, Map<String, Map<Integer, Histogram>> histograms) {
        Map<Integer, MetricSnapshot> existing = metricInfo.get_metrics().get(meta);
        if (existing == null) {
            metricInfo.put_to_metrics(meta, data);
            Map<Integer, Histogram> histogramMap = new HashMap<>();
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Histogram histogram = MetricUtils.metricSnapshot2Histogram(dataEntry.getValue());
                histogramMap.put(dataEntry.getKey(), histogram);
            }
            histograms.put(meta, histogramMap);
        } else {
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Integer win = dataEntry.getKey();
                MetricSnapshot snapshot = dataEntry.getValue();
                MetricSnapshot old = existing.get(win);
                if (old == null) {
                    existing.put(win, snapshot);
                    histograms.get(meta).put(win, MetricUtils.metricSnapshot2Histogram(snapshot));
                } else {
                    if (snapshot.get_ts() >= old.get_ts()) {
                        old.set_ts(snapshot.get_ts());
                        // update points
                        MetricUtils.updateHistogramPoints(histograms.get(meta).get(win), snapshot.get_points());
                    }
                }
            }
        }
        updateMetricCounters(meta, metaCounters);
    }

    /**
     * timers are sampled, we just update points
     */
    public void mergeTimers(MetricInfo metricInfo, String meta, Map<Integer, MetricSnapshot> data,
                            Map<String, Integer> metaCounters, Map<String, Map<Integer, Timer>> timers) {
        Map<Integer, MetricSnapshot> existing = metricInfo.get_metrics().get(meta);
        if (existing == null) {
            metricInfo.put_to_metrics(meta, data);
            Map<Integer, Timer> timerMap = new HashMap<>();
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Timer timer = MetricUtils.metricSnapshot2Timer(dataEntry.getValue());
                timerMap.put(dataEntry.getKey(), timer);
            }
            timers.put(meta, timerMap);
        } else {
            for (Map.Entry<Integer, MetricSnapshot> dataEntry : data.entrySet()) {
                Integer win = dataEntry.getKey();
                MetricSnapshot snapshot = dataEntry.getValue();
                MetricSnapshot old = existing.get(win);
                if (old == null) {
                    existing.put(win, snapshot);
                    timers.get(meta).put(win, MetricUtils.metricSnapshot2Timer(snapshot));
                } else {
                    if (snapshot.get_ts() >= old.get_ts()) {
                        old.set_ts(snapshot.get_ts());
                        old.set_m1(old.get_m1() + snapshot.get_m1());
                        old.set_m5(old.get_m5() + snapshot.get_m5());
                        old.set_m15(old.get_m15() + snapshot.get_m15());

                        // update points
                        MetricUtils.updateTimerPoints(timers.get(meta).get(win), snapshot.get_points());
                    }
                }
            }
        }
        updateMetricCounters(meta, metaCounters);
    }

    /**
     * computes occurrences of specified metric name
     */
    protected void updateMetricCounters(String metricName, Map<String, Integer> metricNameCounters) {
        if (metricNameCounters.containsKey(metricName)) {
            metricNameCounters.put(metricName, metricNameCounters.get(metricName) + 1);
        } else {
            metricNameCounters.put(metricName, 1);
        }
    }

    protected void adjustHistogramTimerMetrics(TopologyMetric tpMetric, Map<String, Integer> metaCounters,
                                               Map<String, Map<Integer, Histogram>> histograms,
                                               Map<String, Map<Integer, Timer>> timers) {
        resetPoints(tpMetric.get_taskMetric().get_metrics());
        resetPoints(tpMetric.get_streamMetric().get_metrics());
        resetPoints(tpMetric.get_nettyMetric().get_metrics());
        resetPoints(tpMetric.get_workerMetric().get_metrics());

        Map<String, Map<Integer, MetricSnapshot>> compMetrics =
                tpMetric.get_componentMetric().get_metrics();
        Map<String, Map<Integer, MetricSnapshot>> topologyMetrics =
                tpMetric.get_topologyMetric().get_metrics();

        adjustMetrics(compMetrics, metaCounters, histograms, timers);
        adjustMetrics(topologyMetrics, metaCounters, histograms, timers);
    }

    private void adjustMetrics(Map<String, Map<Integer, MetricSnapshot>> metrics, Map<String, Integer> metaCounters,
                               Map<String, Map<Integer, Histogram>> histograms, Map<String, Map<Integer, Timer>> timers) {
        for (Map.Entry<String, Map<Integer, MetricSnapshot>> metricEntry : metrics.entrySet()) {
            String meta = metricEntry.getKey();
            MetricType metricType = MetricUtils.metricType(meta);
            MetaType metaType = MetricUtils.metaType(meta);
            Map<Integer, MetricSnapshot> winData = metricEntry.getValue();

            if (metricType == MetricType.HISTOGRAM) {
                for (Map.Entry<Integer, MetricSnapshot> dataEntry : winData.entrySet()) {
                    MetricSnapshot snapshot = dataEntry.getValue();
                    Integer cnt = metaCounters.get(meta);
                    Histogram histogram = histograms.get(meta).get(dataEntry.getKey());
                    if (cnt != null && cnt > 1) {

                        Snapshot snapshot1 = histogram.getSnapshot();
                        snapshot.set_mean(snapshot1.getMean());
                        snapshot.set_p50(snapshot1.getMedian());
                        snapshot.set_p75(snapshot1.get75thPercentile());
                        snapshot.set_p95(snapshot1.get95thPercentile());
                        snapshot.set_p98(snapshot1.get98thPercentile());
                        snapshot.set_p99(snapshot1.get99thPercentile());
                        snapshot.set_p999(snapshot1.get999thPercentile());
                        snapshot.set_stddev(snapshot1.getStdDev());
                        snapshot.set_min(snapshot1.getMin());
                        snapshot.set_max(snapshot1.getMax());

                        if (metaType == MetaType.TOPOLOGY) {
                            snapshot.set_points(Arrays.asList(ArrayUtils.toObject(snapshot1.getValues())));
                        }
                    }
                    if (metaType != MetaType.TOPOLOGY) {
                        snapshot.set_points(new ArrayList<Long>(0));
                    }
                }

            } else if (metricType == MetricType.TIMER) {
                for (Map.Entry<Integer, MetricSnapshot> dataEntry : winData.entrySet()) {
                    MetricSnapshot snapshot = dataEntry.getValue();
                    Integer cnt = metaCounters.get(meta);
                    if (cnt != null && cnt > 1) {
                        Timer timer = timers.get(meta).get(dataEntry.getKey());
                        Snapshot snapshot1 = timer.getSnapshot();
                        snapshot.set_p50(snapshot1.getMedian());
                        snapshot.set_p75(snapshot1.get75thPercentile());
                        snapshot.set_p95(snapshot1.get95thPercentile());
                        snapshot.set_p98(snapshot1.get98thPercentile());
                        snapshot.set_p99(snapshot1.get99thPercentile());
                        snapshot.set_p999(snapshot1.get999thPercentile());
                        snapshot.set_stddev(snapshot1.getStdDev());
                        snapshot.set_min(snapshot1.getMin());
                        snapshot.set_max(snapshot1.getMax());
                    }
                    snapshot.set_points(new ArrayList<Long>(0));
                }
            }
        }
    }

    private void resetPoints(Map<String, Map<Integer, MetricSnapshot>> metrics) {
        for (Map.Entry<String, Map<Integer, MetricSnapshot>> metricEntry : metrics.entrySet()) {
            String meta = metricEntry.getKey();
            MetricType metricType = MetricUtils.metricType(meta);
            Map<Integer, MetricSnapshot> winData = metricEntry.getValue();

            if (metricType == MetricType.HISTOGRAM || metricType == MetricType.TIMER) {
                for (MetricSnapshot snapshot : winData.values()) {
                    snapshot.set_points(new ArrayList<Long>(0));
                }
            }
        }
    }

    protected void adjustCounterMetrics(TopologyMetric tpMetric, TopologyMetric oldMetric) {
        if (oldMetric != null) {
            mergeCounters(tpMetric.get_streamMetric().get_metrics(),
                    oldMetric.get_streamMetric().get_metrics());

            mergeCounters(tpMetric.get_taskMetric().get_metrics(),
                    oldMetric.get_taskMetric().get_metrics());

            mergeCounters(tpMetric.get_componentMetric().get_metrics(),
                    oldMetric.get_componentMetric().get_metrics());

            mergeCounters(tpMetric.get_workerMetric().get_metrics(),
                    oldMetric.get_workerMetric().get_metrics());

            mergeCounters(tpMetric.get_nettyMetric().get_metrics(),
                    oldMetric.get_nettyMetric().get_metrics());
        }
    }

    /**
     * sum old counter snapshots and new counter snapshots, sums are stored in new snapshots.
     */
    private void mergeCounters(Map<String, Map<Integer, MetricSnapshot>> newCounters,
                               Map<String, Map<Integer, MetricSnapshot>> oldCounters) {
        for (Map.Entry<String, Map<Integer, MetricSnapshot>> entry : newCounters.entrySet()) {
            String metricName = entry.getKey();
            Map<Integer, MetricSnapshot> snapshots = entry.getValue();
            Map<Integer, MetricSnapshot> oldSnapshots = oldCounters.get(metricName);
            if (oldSnapshots != null && oldSnapshots.size() > 0) {
                for (Map.Entry<Integer, MetricSnapshot> snapshotEntry : snapshots.entrySet()) {
                    Integer win = snapshotEntry.getKey();
                    MetricSnapshot snapshot = snapshotEntry.getValue();
                    MetricSnapshot oldSnapshot = oldSnapshots.get(win);
                    if (oldSnapshot != null) {
                        snapshot.set_longValue(snapshot.get_longValue() + oldSnapshot.get_longValue());
                    }
                }
            }
        }
    }

    private double getSampleRate() {
        return ConfigExtension.getMetricSampleRate(conf);
    }

}
