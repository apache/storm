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

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cache.JStormCache;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormZkClusterState;
import com.alibaba.jstorm.task.TkHbCacheTime;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

import backtype.storm.Config;
import backtype.storm.scheduler.INimbus;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.TimeCacheMap;

/**
 * All nimbus data
 * 
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
    private NimbusCache cache;

    private int startTime;

    private final ScheduledExecutorService scheduExec;

    private AtomicInteger submittedCount;

    private StatusTransition statusTransition;

    private static final int SCHEDULE_THREAD_NUM = 8;

    private final INimbus inimubs;

    private final boolean localMode;

    private volatile boolean isLeader;

    private AtomicBoolean isShutdown = new AtomicBoolean(false);

    private final TopologyMetricsRunnable metricRunnable;

    // The topologys which has been submitted, but the assignment is not
    // finished
    private TimeCacheMap<String, Object> pendingSubmitTopologys;

    private Map<String, Integer> topologyTaskTimeout;
    
    private TopologyNettyMgr topologyNettyMgr ;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public NimbusData(Map conf, INimbus inimbus) throws Exception {
        this.conf = conf;

        createFileHandler();

        this.submittedCount = new AtomicInteger(0);

        this.stormClusterState = Cluster.mk_storm_cluster_state(conf);

        createCache();

        this.taskHeartbeatsCache =
                new ConcurrentHashMap<String, Map<Integer, TkHbCacheTime>>();

        this.scheduExec = Executors.newScheduledThreadPool(SCHEDULE_THREAD_NUM);

        this.statusTransition = new StatusTransition(this);

        this.startTime = TimeUtils.current_time_secs();

        this.inimubs = inimbus;

        localMode = StormConfig.local_mode(conf);

        this.topologyNettyMgr = new TopologyNettyMgr(conf);
        this.metricRunnable = new TopologyMetricsRunnable(this);

        pendingSubmitTopologys =
                new TimeCacheMap<String, Object>(JStormUtils.MIN_30);
        
        topologyTaskTimeout = new ConcurrentHashMap<String, Integer>();
    }

    /**
     * Just for test
     */
    public NimbusData() {
        scheduExec = Executors.newScheduledThreadPool(6);

        inimubs = null;
        conf = new HashMap<Object, Object>();
        localMode = false;
        this.metricRunnable = new TopologyMetricsRunnable(this);
    }

    public void createFileHandler() {
        TimeCacheMap.ExpiredCallback<Object, Object> expiredCallback =
                new TimeCacheMap.ExpiredCallback<Object, Object>() {
                    @Override
                    public void expire(Object key, Object val) {
                        try {
                            LOG.info("Close file " + String.valueOf(key));
                            if (val != null) {
                                if (val instanceof Channel) {
                                    Channel channel = (Channel) val;
                                    channel.close();
                                } else if (val instanceof BufferFileInputStream) {
                                    BufferFileInputStream is =
                                            (BufferFileInputStream) val;
                                    is.close();
                                }
                            }
                        } catch (IOException e) {
                            LOG.error(e.getMessage(), e);
                        }

                    }
                };

        int file_copy_expiration_secs =
                JStormUtils.parseInt(
                        conf.get(Config.NIMBUS_FILE_COPY_EXPIRATION_SECS), 30);
        uploaders =
                new TimeCacheMap<Object, Object>(file_copy_expiration_secs,
                        expiredCallback);
        downloaders =
                new TimeCacheMap<Object, Object>(file_copy_expiration_secs,
                        expiredCallback);
    }

    public void createCache() throws IOException {
        cache = new NimbusCache(conf, stormClusterState);
        
        ((StormZkClusterState) stormClusterState).setCache(cache.getMemCache());
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

    public Map<Integer, TkHbCacheTime> getTaskHeartbeatsCache(
            String topologyId, boolean createIfNotExist) {
        Map<Integer, TkHbCacheTime> ret = null;
        ret = taskHeartbeatsCache.get(topologyId);
        if (ret == null && createIfNotExist) {
            ret = new ConcurrentHashMap<Integer, TkHbCacheTime>();
            taskHeartbeatsCache.put(topologyId, ret);
        }
        return ret;
    }

    public void setTaskHeartbeatsCache(
            ConcurrentHashMap<String, Map<Integer, TkHbCacheTime>> taskHeartbeatsCache) {
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
        cache.cleanup();
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
        return cache.getMemCache();
    }
    
    public JStormCache getDbCache() {
        return cache.getDbCache();
    }
    
    public NimbusCache getNimbusCache() {
        return cache;
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

	public TopologyNettyMgr getTopologyNettyMgr() {
		return topologyNettyMgr;
	}
    
    
}
