package com.alibaba.jstorm.metric;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.cache.JStormCache;
import com.alibaba.jstorm.cache.RocksDBCache;
import com.alibaba.jstorm.cache.TimeoutMemCache;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.OSInfo;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * metrics cache. we maintain the following data in rocks DB cache: 1. all topology ids 2. topology id ==> all metrics meta(map<metric_name, metric_id>) 3.
 * topology id ==> all metrics data
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
@SuppressWarnings("unchecked")
public class JStormMetricCache {

    private static final Logger LOG = LoggerFactory.getLogger(JStormMetricCache.class);

    public static final String TIMEOUT_MEM_CACHE_CLASS = TimeoutMemCache.class.getName();
    public static final String ROCKS_DB_CACHE_CLASS = RocksDBCache.class.getName();

    protected final Object lock = new Object();

    protected JStormCache cache = null;

    protected static final String METRIC_META_PREFIX = "__metric.meta__";
    protected static final String SENT_METRIC_META_PREFIX = "__saved.metric.meta__";
    protected static final String ALL_TOPOLOGIES_KEY = "__all.topologies__";
    protected static final String TOPOLOGY_SAMPLE_RATE = "__topology.sample.rate__";

    protected static final String METRIC_DATA_PREFIX = "__metric.data__";
    protected static final String METRIC_DATA_30M_COMPONENT = "__metric.data.comp__";
    protected static final String METRIC_DATA_30M_TASK = "__metric.data.task__";
    protected static final String METRIC_DATA_30M_STREAM = "__metric.data.stream__";
    protected static final String METRIC_DATA_30M_WORKER = "__metric.data.worker__";
    protected static final String METRIC_DATA_30M_NETTY = "__metric.data.netty__";
    protected static final String METRIC_DATA_30M_TOPOLOGY = "__metric.data.topology__";

    protected final StormClusterState zkCluster;

    public String getNimbusCacheClass(Map conf) {
        boolean isLinux = OSInfo.isLinux();
        boolean isMac = OSInfo.isMac();
        boolean isLocal = StormConfig.local_mode(conf);

        if (isLocal) {
            return TIMEOUT_MEM_CACHE_CLASS;
        }

        if (!isLinux && !isMac) {
            return TIMEOUT_MEM_CACHE_CLASS;
        }

        String nimbusCacheClass = ConfigExtension.getNimbusCacheClass(conf);
        if (!StringUtils.isBlank(nimbusCacheClass)) {
            return nimbusCacheClass;
        }

        return ROCKS_DB_CACHE_CLASS;
    }

    public JStormMetricCache(Map conf, StormClusterState zkCluster) {
        String dbCacheClass = getNimbusCacheClass(conf);
        LOG.info("JStorm metrics cache will use {}", dbCacheClass);

        boolean reset = ConfigExtension.getMetricCacheReset(conf);
        try {
            cache = (JStormCache) Utils.newInstance(dbCacheClass);

            String dbDir = StormConfig.metricDbDir(conf);
            conf.put(RocksDBCache.ROCKSDB_ROOT_DIR, dbDir);
            conf.put(RocksDBCache.ROCKSDB_RESET, reset);
            cache.init(conf);
        } catch (Exception e) {
            if (!reset && cache != null) {
                LOG.error("Failed to init rocks db, will reset and try to re-init...");
                conf.put(RocksDBCache.ROCKSDB_RESET, true);
                try {
                    cache.init(conf);
                } catch (Exception ex) {
                    LOG.error("Error", ex);
                }
            } else {
                LOG.error("Failed to create metrics cache!", e);
                throw new RuntimeException(e);
            }
        }

        this.zkCluster = zkCluster;
    }

    public JStormCache getCache() {
        return cache;
    }

    public JStormCache put(String k, Object v) {
        cache.put(k, v);
        return cache;
    }

    /**
     * store 30min metric data. the metric data is stored in a ring.
     */
    public JStormCache putMetricData(String topologyId, TopologyMetric tpMetric) {
        // map<key, [ts, metric_info]>
        Map<String, Object> batchData = new HashMap<String, Object>();
        long ts = System.currentTimeMillis();
        int tp = 0, comp = 0, task = 0, stream = 0, worker = 0, netty = 0;
        if (tpMetric.get_componentMetric().get_metrics_size() > 0) {
            batchData.put(METRIC_DATA_30M_COMPONENT + topologyId, new Object[]{ts, tpMetric.get_componentMetric()});
            comp += tpMetric.get_componentMetric().get_metrics_size();
        }
        if (tpMetric.get_taskMetric().get_metrics_size() > 0) {
            tryCombineMetricInfo(METRIC_DATA_30M_TASK + topologyId, tpMetric.get_taskMetric(), MetaType.TASK, ts);
            task += tpMetric.get_taskMetric().get_metrics_size();
        }
        if (tpMetric.get_streamMetric().get_metrics_size() > 0) {
            tryCombineMetricInfo(METRIC_DATA_30M_STREAM + topologyId, tpMetric.get_streamMetric(), MetaType.STREAM, ts);
            stream += tpMetric.get_streamMetric().get_metrics_size();
        }
        if (tpMetric.get_workerMetric().get_metrics_size() > 0) {
            tryCombineMetricInfo(METRIC_DATA_30M_WORKER + topologyId, tpMetric.get_workerMetric(), MetaType.WORKER, ts);
            worker += tpMetric.get_workerMetric().get_metrics_size();
        }
        if (tpMetric.get_nettyMetric().get_metrics_size() > 0) {
            tryCombineMetricInfo(METRIC_DATA_30M_NETTY + topologyId, tpMetric.get_nettyMetric(), MetaType.NETTY, ts);
            netty += tpMetric.get_nettyMetric().get_metrics_size();
        }

        // store 30 snapshots of topology metrics
        if (tpMetric.get_topologyMetric().get_metrics_size() > 0) {
            String keyPrefix = METRIC_DATA_30M_TOPOLOGY + topologyId + "-";
            int page = getRingAvailableIndex(keyPrefix);

            batchData.put(keyPrefix + page, new Object[]{ts, tpMetric.get_topologyMetric()});
            tp += tpMetric.get_topologyMetric().get_metrics_size();
        }
        LOG.info("caching metric data for topology:{},tp:{},comp:{},task:{},stream:{},worker:{},netty:{},cost:{}",
                topologyId, tp, comp, task, stream, worker, netty, System.currentTimeMillis() - ts);

        return putBatch(batchData);
    }

    private int getRingAvailableIndex(String keyPrefix) {
        int page = 0;
        // backward check
        long last_ts = 0;
        for (int idx = 1; idx <= 30; idx++) {
            String key = keyPrefix + idx;
            if (cache.get(key) != null) {
                long timestamp = (long) ((Object[]) cache.get(key))[0];
                if (timestamp > last_ts) {
                    last_ts = timestamp;
                    page = idx;
                }
            }
        }
        if (page < 30) {
            page += 1;
        } else {
            page = 1;
        }
        return page;
    }

    private void tryCombineMetricInfo(String key, MetricInfo incoming, MetaType metaType, long ts) {
        Object data = cache.get(key);
        if (data != null) {
            try {
                Object[] parts = (Object[]) data;
                MetricInfo old = (MetricInfo) parts[1];

                LOG.info("combine {} metrics, old:{}, new:{}",
                        metaType, old.get_metrics_size(), incoming.get_metrics_size());
                old.get_metrics().putAll(incoming.get_metrics());
                // remove dead worker
                cache.put(key, new Object[]{ts, old});
            } catch (Exception ignored) {
                cache.remove(key);
                cache.put(key, new Object[]{ts, incoming});
            }
        } else {
            cache.put(key, new Object[]{ts, incoming});
        }
    }

    public List<MetricInfo> getMetricData(String topologyId, MetaType metaType) {
        Map<Long, MetricInfo> retMap = new TreeMap<Long, MetricInfo>();

        String key = null;
        if (metaType == MetaType.COMPONENT) {
            key = METRIC_DATA_30M_COMPONENT + topologyId;
        } else if (metaType == MetaType.TASK) {
            key = METRIC_DATA_30M_TASK + topologyId;
        } else if (metaType == MetaType.STREAM) {
            key = METRIC_DATA_30M_STREAM + topologyId;
        } else if (metaType == MetaType.WORKER) {
            key = METRIC_DATA_30M_WORKER + topologyId;
        } else if (metaType == MetaType.NETTY) {
            key = METRIC_DATA_30M_NETTY + topologyId;
        } else if (metaType == MetaType.TOPOLOGY) {
            String keyPrefix = METRIC_DATA_30M_TOPOLOGY + topologyId + "-";
            for (int i = 1; i <= 30; i++) {
                Object obj = cache.get(keyPrefix + i);
                if (obj != null) {
                    Object[] objects = (Object[]) obj;
                    retMap.put((Long) objects[0], (MetricInfo) objects[1]);
                }
            }
        }
        if (key != null) {
            Object obj = cache.get(key);
            if (obj != null) {
                Object[] objects = (Object[]) obj;
                retMap.put((Long) objects[0], (MetricInfo) objects[1]);
            }
        }
        List<MetricInfo> ret = Lists.newArrayList(retMap.values());
        int cnt = 0;
        for (MetricInfo metricInfo : ret) {
            cnt += metricInfo.get_metrics_size();
        }
        LOG.info("getMetricData, topology:{}, meta type:{}, metric info size:{}, total metric size:{}",
                topologyId, metaType, ret.size(), cnt);
        return ret;
    }

    public JStormCache putBatch(Map<String, Object> kv) {
        if (kv.size() > 0) {
            cache.putBatch(kv);
        }
        return cache;
    }

    public Object get(String k) {
        return cache.get(k);
    }

    public void remove(String k) {
        cache.remove(k);
    }

    public void removeTopology(String topologyId) {
        removeTopologyMeta(topologyId);
        removeTopologyData(topologyId);
    }

    protected void removeTopologyMeta(String topologyId) {
        cache.remove(METRIC_META_PREFIX + topologyId);
    }

    protected void removeTopologyData(String topologyId) {
        long start = System.currentTimeMillis();
        cache.remove(METRIC_DATA_PREFIX + topologyId);

        Set<String> metricDataKeys = new HashSet<>();
        for (int i = 1; i <= 30; i++) {
            String metricDataKeySuffix = topologyId + "-" + i;
            metricDataKeys.add(METRIC_DATA_30M_TOPOLOGY + metricDataKeySuffix);
        }
        metricDataKeys.add(METRIC_DATA_30M_COMPONENT + topologyId);
        metricDataKeys.add(METRIC_DATA_30M_TASK + topologyId);
        metricDataKeys.add(METRIC_DATA_30M_STREAM + topologyId);
        metricDataKeys.add(METRIC_DATA_30M_WORKER + topologyId);
        metricDataKeys.add(METRIC_DATA_30M_NETTY + topologyId);

        cache.removeBatch(metricDataKeys);
        LOG.info("removing metric cache of topology:{}, cost:{}", topologyId, System.currentTimeMillis() - start);
    }

    public void unregisterWorker(String topologyId, String host, int port) {
        String prefix = MetricUtils.workerMetricPrefix(topologyId, host, port);
        synchronized (lock) {
            //remove dead worker meta info in METRIC_META_PREFIX
            Map<String, Long> nodes = (Map<String, Long>) cache.get(METRIC_META_PREFIX + topologyId);
            if (nodes != null) {
                Iterator<String> keyIterator = nodes.keySet().iterator();
                while (keyIterator.hasNext()){
                    String metricName = keyIterator.next();
                    // remove metric type
                    metricName = metricName.charAt(0) + metricName.substring(2, metricName.length());
                    if (metricName.startsWith(prefix)) {
                        keyIterator.remove();
                    }
                }
                cache.put(METRIC_META_PREFIX + topologyId, nodes);
            }
            //remove dead worker in METRIC_DATA_30M_WORKER
            Object data = cache.get(METRIC_DATA_30M_WORKER + topologyId);
            if (data != null) {
                Object[] parts = (Object[]) data;
                MetricInfo old = (MetricInfo) parts[1];
                Iterator<String> oldKeys = old.get_metrics().keySet().iterator();
                while (oldKeys.hasNext()) {
                    String metricName = oldKeys.next();
                    metricName = metricName.charAt(0) + metricName.substring(2, metricName.length());
                    if (metricName.startsWith(prefix)) {
                        oldKeys.remove();
                        LOG.info("remove dead worker metric : {}", metricName);
                    }
                }
                cache.put(METRIC_DATA_30M_WORKER + topologyId, data);
            }
        }
    }

    public Map<String, Long> getMeta(String topologyId) {
        return (Map<String, Long>) cache.get(METRIC_META_PREFIX + topologyId);
    }

    public void putMeta(String topologyId, Object v) {
        cache.put(METRIC_META_PREFIX + topologyId, v);
    }

    public void putSampleRate(String topologyId, double sampleRate) {
        cache.put(TOPOLOGY_SAMPLE_RATE + topologyId, sampleRate);
    }

    public void removeSampleRate(String topologyId) {
        cache.remove(TOPOLOGY_SAMPLE_RATE + topologyId);
    }

    public double getSampleRate(String topologyId) {
        String rate = (String) cache.get(TOPOLOGY_SAMPLE_RATE + topologyId);
        if (rate == null) {
            return ConfigExtension.DEFAULT_METRIC_SAMPLE_RATE;
        }
        return Double.parseDouble(rate);
    }

    public Map<String, Long> getSentMeta(String topologyId) {
        return (Map<String, Long>) cache.get(SENT_METRIC_META_PREFIX + topologyId);
    }

    public void putSentMeta(String topologyId, Object allMetricMeta) {
        cache.put(SENT_METRIC_META_PREFIX + topologyId, allMetricMeta);
    }
}
