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
package com.alibaba.jstorm.daemon.nimbus;

import backtype.storm.Config;
import backtype.storm.generated.TopologyTaskHbInfo;
import backtype.storm.scheduler.INimbus;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.TimeCacheMap;
import com.alibaba.jstorm.cache.JStormCache;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormZkClusterState;
import com.alibaba.jstorm.metric.JStormMetricCache;
import com.alibaba.jstorm.metric.JStormMetricsReporter;
import com.alibaba.jstorm.task.TkHbCacheTime;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * All nimbus data
 */
public class NimbusData {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusData.class);

    private Map<Object, Object> conf;

    private StormClusterState stormClusterState;

    // Map<topologyId, Map<taskid, TkHbCacheTime>>
    private ConcurrentHashMap<String, Map<Integer, TkHbCacheTime>> taskHeartbeatsCache;

    // TODO two kind of value:Channel/BufferFileInputStream
    private TimeCacheMap<Object, Object> downloaders;
    private TimeCacheMap<Object, Object> uploaders;
    // cache thrift response to avoid scan zk too frequently
    private NimbusCache nimbusCache;

    private int startTime;

    private final ScheduledExecutorService scheduExec;

    private AtomicInteger submittedCount;

    private StatusTransition statusTransition;

    private static final int SCHEDULE_THREAD_NUM = 8;

    private final INimbus inimubs;

    private final boolean localMode;

    private volatile boolean isLeader;

    private AtomicBoolean isShutdown = new AtomicBoolean(false);

    private TopologyMetricsRunnable metricRunnable;
    private AsyncLoopThread metricLoopThread;

    // The topologys which has been submitted, but the assignment is not
    // finished
    private TimeCacheMap<String, Object> pendingSubmitTopologys;
    private Map<String, Integer> topologyTaskTimeout;

    // Map<TopologyId, TasksHeartbeat>
    private Map<String, TopologyTaskHbInfo> tasksHeartbeat;

    private final JStormMetricCache metricCache;

    private final String clusterName;

    private JStormMetricsReporter metricsReporter;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public NimbusData(Map conf, INimbus inimbus) throws Exception {
        this.conf = conf;

        createFileHandler();

        this.submittedCount = new AtomicInteger(0);

        this.stormClusterState = Cluster.mk_storm_cluster_state(conf);

        createCache();

        this.taskHeartbeatsCache = new ConcurrentHashMap<String, Map<Integer, TkHbCacheTime>>();

        this.scheduExec = Executors.newScheduledThreadPool(SCHEDULE_THREAD_NUM);

        this.statusTransition = new StatusTransition(this);

        this.startTime = TimeUtils.current_time_secs();

        this.inimubs = inimbus;

        localMode = StormConfig.local_mode(conf);

        this.metricCache = new JStormMetricCache(conf, this.stormClusterState);
        this.clusterName = ConfigExtension.getClusterName(conf);

        this.metricRunnable = new TopologyMetricsRunnable(this);
        this.metricRunnable.init();

        pendingSubmitTopologys = new TimeCacheMap<String, Object>(JStormUtils.MIN_30);
        topologyTaskTimeout = new ConcurrentHashMap<String, Integer>();
        tasksHeartbeat = new ConcurrentHashMap<String, TopologyTaskHbInfo>();

        if (!localMode) {
            startMetricThreads();
        }
    }

    public void startMetricThreads() {
        this.metricRunnable.start();

        // init nimbus metric reporter
        this.metricsReporter = new JStormMetricsReporter(this);
        this.metricsReporter.init();
    }

    public void createFileHandler() {
        TimeCacheMap.ExpiredCallback<Object, Object> expiredCallback = new TimeCacheMap.ExpiredCallback<Object, Object>() {
            @Override
            public void expire(Object key, Object val) {
                try {
                    LOG.info("Close file " + String.valueOf(key));
                    if (val != null) {
                        if (val instanceof Channel) {
                            Channel channel = (Channel) val;
                            channel.close();
                        } else if (val instanceof BufferFileInputStream) {
                            BufferFileInputStream is = (BufferFileInputStream) val;
                            is.close();
                        }
                    }
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }

            }
        };

        int file_copy_expiration_secs = JStormUtils.parseInt(conf.get(Config.NIMBUS_FILE_COPY_EXPIRATION_SECS), 30);
        uploaders = new TimeCacheMap<Object, Object>(file_copy_expiration_secs, expiredCallback);
        downloaders = new TimeCacheMap<Object, Object>(file_copy_expiration_secs, expiredCallback);
    }

    public void createCache() throws IOException {
        nimbusCache = new NimbusCache(conf, stormClusterState);
        ((StormZkClusterState) stormClusterState).setCache(nimbusCache.getMemCache());
    }

    public String getClusterName() {
        return clusterName;
    }

    public int uptime() {
        return (TimeUtils.current_time_secs() - startTime);
    }

    public Map<Object, Object> getConf() {
        return conf;
    }

    public void setConf(Map<Object, Object> conf) {
        this.conf = conf;
    }

    public StormClusterState getStormClusterState() {
        return stormClusterState;
    }

    public void setStormClusterState(StormClusterState stormClusterState) {
        this.stormClusterState = stormClusterState;
    }

    public ConcurrentHashMap<String, Map<Integer, TkHbCacheTime>> getTaskHeartbeatsCache() {
        return taskHeartbeatsCache;
    }

    public Map<Integer, TkHbCacheTime> getTaskHeartbeatsCache(String topologyId, boolean createIfNotExist) {
        Map<Integer, TkHbCacheTime> ret = null;
        ret = taskHeartbeatsCache.get(topologyId);
        if (ret == null && createIfNotExist) {
            ret = new ConcurrentHashMap<Integer, TkHbCacheTime>();
            taskHeartbeatsCache.put(topologyId, ret);
        }
        return ret;
    }

    public void setTaskHeartbeatsCache(ConcurrentHashMap<String, Map<Integer, TkHbCacheTime>> taskHeartbeatsCache) {
        this.taskHeartbeatsCache = taskHeartbeatsCache;
    }

    public TimeCacheMap<Object, Object> getDownloaders() {
        return downloaders;
    }

    public void setDownloaders(TimeCacheMap<Object, Object> downloaders) {
        this.downloaders = downloaders;
    }

    public TimeCacheMap<Object, Object> getUploaders() {
        return uploaders;
    }

    public void setUploaders(TimeCacheMap<Object, Object> uploaders) {
        this.uploaders = uploaders;
    }

    public int getStartTime() {
        return startTime;
    }

    public void setStartTime(int startTime) {
        this.startTime = startTime;
    }

    public AtomicInteger getSubmittedCount() {
        return submittedCount;
    }

    public ScheduledExecutorService getScheduExec() {
        return scheduExec;
    }

    public StatusTransition getStatusTransition() {
        return statusTransition;
    }

    public void cleanup() {
        nimbusCache.cleanup();
        LOG.info("Successfully shutdown Cache");
        try {
            stormClusterState.disconnect();
            LOG.info("Successfully shutdown ZK Cluster Instance");
        } catch (Exception e) {
            // TODO Auto-generated catch block

        }
        try {
            scheduExec.shutdown();
            LOG.info("Successfully shutdown threadpool");
        } catch (Exception e) {
        }

        uploaders.cleanup();
        downloaders.cleanup();
    }

    public INimbus getInimubs() {
        return inimubs;
    }

    public boolean isLocalMode() {
        return localMode;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    public AtomicBoolean getIsShutdown() {
        return isShutdown;
    }

    public JStormCache getMemCache() {
        return nimbusCache.getMemCache();
    }

    public JStormCache getDbCache() {
        return nimbusCache.getDbCache();
    }

    public NimbusCache getNimbusCache() {
        return nimbusCache;
    }

    public JStormMetricCache getMetricCache() {
        return metricCache;
    }

    public final TopologyMetricsRunnable getMetricRunnable() {
        return metricRunnable;
    }

    public TimeCacheMap<String, Object> getPendingSubmitTopoloygs() {
        return pendingSubmitTopologys;
    }

    public Map<String, Integer> getTopologyTaskTimeout() {
        return topologyTaskTimeout;
    }

    public Map<String, TopologyTaskHbInfo> getTasksHeartbeat() {
        return tasksHeartbeat;
    }
}
