package com.alipay.dw.jstorm.daemon.nimbus;

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.Config;
import backtype.storm.generated.NotAliveException;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.TimeCacheMap;

import com.alipay.dw.jstorm.cluster.Cluster;
import com.alipay.dw.jstorm.cluster.Common;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.task.TkHbCacheTime;
import com.alipay.dw.jstorm.utils.PathUtils;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.utils.TimeUtils;

/**
 * All nimbus data
 * 
 */
public class NimbusData {
    
    private Map<Object, Object>                                    conf;
    
    private StormClusterState                                      stormClusterState;
    
    // Map<topologyId, Map<taskid, TkHbCacheTime>>
    private ConcurrentHashMap<String, Map<Integer, TkHbCacheTime>> taskHeartbeatsCache;
    
    // TODO two kind of value:Channel/BufferFileInputStream
    private TimeCacheMap<Object, Object>                           downloaders;
    private TimeCacheMap<Object, Object>                           uploaders;
    
    private int                                                    startTime;
    
    private final ScheduledExecutorService                         scheduExec;
    
    private AtomicInteger                                          submittedCount;
    
    private Object                                                 submitLock          = new Object();
    
    private StatusTransition                                       statusTransition;
    
    private static final int                                       SCHEDULE_THREAD_NUM = 8;
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public NimbusData(Map conf, TimeCacheMap<Object, Object> downloaders,
            TimeCacheMap<Object, Object> uploaders) throws Exception {
        this.conf = conf;
        this.downloaders = downloaders;
        this.uploaders = uploaders;
        
        this.submittedCount = new AtomicInteger(0);
        
        this.stormClusterState = Cluster.mk_storm_cluster_state(conf);
        
        this.taskHeartbeatsCache = new ConcurrentHashMap<String, Map<Integer, TkHbCacheTime>>();
        
        this.scheduExec = Executors.newScheduledThreadPool(SCHEDULE_THREAD_NUM);
        
        this.statusTransition = new StatusTransition(this);
        
        this.startTime = TimeUtils.current_time_secs();
    }
    
    /**
     * for test
     */
    public NimbusData() {
        scheduExec = Executors.newScheduledThreadPool(6);
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
    
    public void setSubmittedCount(AtomicInteger submittedCount) {
        this.submittedCount = submittedCount;
    }
    
    public Object getSubmitLock() {
        return submitLock;
    }
    
    public ScheduledExecutorService getScheduExec() {
        return scheduExec;
    }
    
    public StatusTransition getStatusTransition() {
        return statusTransition;
    }
    
    public void cleanup() {
        try {
            stormClusterState.disconnect();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            
        }
        scheduExec.shutdown();
    }
    
}
