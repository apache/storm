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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cache.JStormCache;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.common.metric.Histogram;
import com.alibaba.jstorm.metric.AlimonitorClient;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricSendClient;
import com.alibaba.jstorm.metric.MetricThrift;
import com.alibaba.jstorm.metric.SimpleJStormMetric;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.utils.TimeCacheMap;
import com.codahale.metrics.Gauge;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricWindow;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.generated.WorkerUploadMetrics;

public class TopologyMetricsRunnable extends RunnableCallback {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyMetricsRunnable.class);
    private static final String DEAD_SUPERVISOR_HEAD = "DeadSupervisor-";
    
    public static interface Event {
        
    }
    
    public static class Update implements Event {
        public WorkerUploadMetrics workerMetrics;
    }
    
    public static class Remove implements Event {
        public String topologyId;
    }
    
    public static class Upload implements Event {
        public long timeStamp;
    }
    
    public static final String CACHE_NAMESPACE_METRIC = "cache_namespace_metric";
    public static final String CACHE_NAMESPACE_NETTY = "cache_namespace_netty";
    protected NimbusCache nimbusCache;
    protected JStormCache dbCache;
    
    /**
     * cache all worker metrics will waste a little memory
     * 
     */
    protected Map<String, Set<String>> topologyWorkers;
    protected TimeCacheMap<String, Long> removing;
    
    protected BlockingDeque<TopologyMetricsRunnable.Event> queue;
    protected StormClusterState stormClusterState;
    
    protected MetricSendClient metricSendClient;
    protected TopologyMetric emptyTopologyMetric = mkTopologyMetric();
    protected TreeMap<String, MetricInfo>   emptyNettyMetric = new TreeMap<String, MetricInfo>();
    protected AtomicBoolean isShutdown;
    protected boolean localMode;
    protected TopologyNettyMgr topologyNettyMgr;
    
    protected Histogram updateHistogram;
    protected AtomicBoolean isUploading = new AtomicBoolean(false);
    protected Histogram uploadHistogram;
    
    public TopologyMetricsRunnable(NimbusData nimbusData) {
        
        this.nimbusCache = nimbusData.getNimbusCache();
        this.dbCache = nimbusCache.getDbCache();
        this.topologyWorkers = new ConcurrentHashMap<String, Set<String>>();
        this.removing = new TimeCacheMap<String, Long>(600);
        this.queue = new LinkedBlockingDeque<TopologyMetricsRunnable.Event>();
        this.stormClusterState = nimbusData.getStormClusterState();
        this.isShutdown = nimbusData.getIsShutdown();
        this.topologyNettyMgr = nimbusData.getTopologyNettyMgr();
        
        if (ConfigExtension.isAlimonitorMetricsPost(nimbusData.getConf())) {
            metricSendClient = new AlimonitorClient(AlimonitorClient.DEFAUT_ADDR, AlimonitorClient.DEFAULT_PORT, true);
        } else {
            metricSendClient = new MetricSendClient();
        }
        localMode = StormConfig.local_mode(nimbusData.getConf());
        
        updateHistogram = SimpleJStormMetric.registerHistorgram("TopologyMetricsRunnable_Update");
        uploadHistogram = SimpleJStormMetric.registerHistorgram("TopologyMetricsRunnable_Upload");
        
        SimpleJStormMetric.registerWorkerGauge(new Gauge<Double>() {
            
            @Override
            public Double getValue() {
                // TODO Auto-generated method stub
                return (double) queue.size();
            }
        }, "TopologyMetricsRunnable_Queue");
    }
    
    public void pushEvent(TopologyMetricsRunnable.Event cmd) {
        queue.offer(cmd);
    }
    
    public TopologyMetric mkTopologyMetric() {
        TopologyMetric emptyTopologyMetric = new TopologyMetric();
        
        MetricInfo topologyMetricInfo = MetricThrift.mkMetricInfo();
        emptyTopologyMetric.set_topologyMetric(topologyMetricInfo);
        
        emptyTopologyMetric.set_componentMetric(new HashMap<String, MetricInfo>());
        emptyTopologyMetric.set_workerMetric(new HashMap<String, MetricInfo>());
        emptyTopologyMetric.set_taskMetric(new HashMap<Integer, MetricInfo>());
        return emptyTopologyMetric;
    }
    
    @Override
    public void run() {
        try {
            TopologyMetricsRunnable.Event event = queue.take();
            
            if (event instanceof Remove) {
                
                handleRemoveEvent((Remove) event);
                return;
            } else if (event instanceof Update) {
                handleUpdateEvent((Update) event);
                return;
            } else if (event instanceof Upload) {
                handleUploadEvent((Upload) event);
                return;
            } else {
                LOG.error("Unknow event type");
                return;
            }
            
        } catch (Exception e) {
            if (isShutdown.get() == false) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
    
    public void handleRemoveEvent(Remove event) {
        String topologyId = event.topologyId;
        TopologyMetric topologyMetric = (TopologyMetric) dbCache.get(getTopologyKey(topologyId));
        if (topologyMetric == null) {
            LOG.warn("No TopologyMetric of  " + topologyId);
            return;
        }
        
        removing.put(topologyId, System.currentTimeMillis());
        dbCache.remove(getTopologyKey(topologyId));
        dbCache.remove(getNettyTopologyKey(topologyId));
        topologyNettyMgr.rmTopology(topologyId);
        LOG.info("Successfully remove TopologyMetric of " + topologyId);
        return;
        
    }
    
    public void cleanDeadSupervisorWorker(TopologyMetric metric) {
        List<String> removeList = new ArrayList<String>();
        
        Map<String, MetricInfo> workerMetric = metric.get_workerMetric();
        if (workerMetric == null) {
            return;
        }
        for (String hostPort : workerMetric.keySet()) {
            if (hostPort.startsWith(DEAD_SUPERVISOR_HEAD)) {
                removeList.add(hostPort);
            }
        }
        
        for (String removed : removeList) {
            workerMetric.remove(removed);
        }
    }
    
    public void cleanTopology() {
        Map<String, Long> removingMap = removing.buildMap();
        
        Map<String, Assignment> assignMap = null;
        try {
            assignMap = Cluster.get_all_assignment(stormClusterState, null);
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            LOG.info("Failed to get Assignments");
        }
        
        for (String topologyId : topologyWorkers.keySet()) {
            if (assignMap.containsKey(topologyId) == false) {
                removingMap.put(topologyId, System.currentTimeMillis());
            }
        }
        
        for (String topologyId : removingMap.keySet()) {
            dbCache.remove(getTopologyKey(topologyId));
            
            Set<String> workers = topologyWorkers.get(topologyId);
            if (workers != null) {
                for (String workerSlot : workers) {
                    dbCache.remove(getWorkerKey(topologyId, workerSlot));
                }
                topologyWorkers.remove(topologyId);
            }
            
        }
        
        for (Entry<String, Set<String>> entry : topologyWorkers.entrySet()) {
            String topologyId = entry.getKey();
            Set<String> metricWorkers = entry.getValue();
            
            Set<String> workerSlots = new HashSet<String>();
            
            Assignment assignment = assignMap.get(topologyId);
            if (assignment == null) {
                LOG.error("Assignment disappear of " + topologyId);
                continue;
            }
            
            for (ResourceWorkerSlot worker : assignment.getWorkers()) {
                String slot = getWorkerSlotName(worker.getNodeId(), worker.getPort());
                workerSlots.add(slot);
            }
            
            Set<String> removes = new HashSet<String>();
            for (String slot : metricWorkers) {
                if (workerSlots.contains(slot) == false) {
                    LOG.info("Remove worker metrics of {}:{}", topologyId, slot);
                    removes.add(slot);
                }
            }
            
            for (String slot : removes) {
                metricWorkers.remove(slot);
                dbCache.remove(getWorkerKey(topologyId, slot));
            }
        }
    }
    
    /**
     * Upload metric to ZK
     * 
     * @param event
     */
    public void handleUploadEvent(Upload event) {
        if (isUploading.getAndSet(true) == true) {
            LOG.info("Nimbus is alread uploading");
            return ;
        }
        
        long start = System.currentTimeMillis();
        
        cleanTopology();
        
        render();
        
        isUploading.set(false);
        
        long end = System.currentTimeMillis();
        uploadHistogram.update(end - start);
        
        
    }
    
    public String getWorkerHostname(WorkerUploadMetrics workerMetrics) {
        
        String hostname = null;
        String supervisorId = workerMetrics.get_supervisor_id();
        try {
            hostname = Cluster.get_supervisor_hostname(stormClusterState, supervisorId);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.warn("Failed to get hostname of " + supervisorId);
        }
        if (hostname == null) {
            hostname = DEAD_SUPERVISOR_HEAD + supervisorId;
        }
        
        return hostname;
    }
    
    public void avgMetricWindow(MetricWindow metric, int parallel) {
        if (parallel == 0) {
            return;
        }
        Map<Integer, Double> map = metric.get_metricWindow();
        Map<Integer, Double> newMap = new HashMap<Integer, Double>();
        if (map != null) {
            for (Entry<Integer, Double> entry : map.entrySet()) {
                newMap.put(entry.getKey(), entry.getValue() / parallel);
            }
        }
        
        metric.set_metricWindow(newMap);
    }
    
    public MetricInfo mergeMetricInfo(MetricInfo from, MetricInfo to, Set<String> tags) {
        if (to == null) {
            to = MetricThrift.mkMetricInfo();
        }
        
        if (from.get_baseMetric() == null) {
            LOG.warn("No base Metric ");
            return to;
        }
        
        for (String tag : tags) {
            
            MetricWindow fromMetric = from.get_baseMetric().get(tag);
            Map<String, MetricWindow> toMetricMap = to.get_baseMetric();
            if (toMetricMap == null) {
                toMetricMap = new HashMap<String, MetricWindow>();
                to.set_baseMetric(toMetricMap);
            }
            
            MetricWindow toMetric = toMetricMap.get(tag);
            
            toMetric = MetricThrift.mergeMetricWindow(fromMetric, toMetric);
            
            toMetricMap.put(tag, toMetric);
            
        }
        
        return to;
    }
    
    public Map<String, Map<String, MetricWindow>> mergeTaskStreams(
            Map<String, Map<String, MetricWindow>> componentStreams,
            Map<String, Map<String, MetricWindow>> taskStreams,
            Map<String, Map<String, AtomicInteger>> componentStreamParallel) {
        
        if (taskStreams == null || taskStreams.size() == 0) {
            return componentStreams;
        }
        
        if (componentStreams == null) {
            componentStreams = new HashMap<String, Map<String, MetricWindow>>();
        }
        
        for (Entry<String, Map<String, MetricWindow>> entry : taskStreams.entrySet()) {
            String metricName = entry.getKey();
            Map<String, MetricWindow> streamMetricWindows = entry.getValue();
            
            if (streamMetricWindows == null) {
                continue;
            }
            
            Map<String, AtomicInteger> streamCounters = componentStreamParallel.get(metricName);
            if (streamCounters == null) {
                streamCounters = new HashMap<String, AtomicInteger>();
                componentStreamParallel.put(metricName, streamCounters);
            }
            
            Map<String, MetricWindow> componentStreamMetricWindows = componentStreams.get(metricName);
            if (componentStreamMetricWindows == null) {
                componentStreamMetricWindows = new HashMap<String, MetricWindow>();
                componentStreams.put(metricName, componentStreamMetricWindows);
            }
            
            for (Entry<String, MetricWindow> streamEntry : streamMetricWindows.entrySet()) {
                String streamName = streamEntry.getKey();
                MetricWindow taskMetricWindow = streamEntry.getValue();
                
                MetricWindow componentMetricWindow = componentStreamMetricWindows.get(streamName);
                
                componentMetricWindow = MetricThrift.mergeMetricWindow(taskMetricWindow, componentMetricWindow);
                
                componentStreamMetricWindows.put(streamName, componentMetricWindow);
                
                AtomicInteger counter = streamCounters.get(streamName);
                if (counter == null) {
                    counter = new AtomicInteger(0);
                    streamCounters.put(streamName, counter);
                }
                counter.incrementAndGet();
            }
        }
        
        return componentStreams;
    }
    
    public void avgStreams(Map<String, Map<String, MetricWindow>> tagStreamsMetrics, Map<String, Map<String, AtomicInteger>> counters, String tag) {
        if (tagStreamsMetrics == null) {
            return;
        }
        
        Map<String, MetricWindow> streamMetrics = tagStreamsMetrics.get(tag);
        if (streamMetrics == null) {
            return;
        }
        
        for (Entry<String, MetricWindow> entry : streamMetrics.entrySet()) {
            String streamName = entry.getKey();
            MetricWindow metric = entry.getValue();
            
            AtomicInteger counter = counters.get(tag).get(streamName);
            if (counter == null) {
                continue;
                
            }
            
            avgMetricWindow(metric, counter.get());
        }
    }
    
    public void mergeTasks(TopologyMetric topologyMetric, String topologyId) {
        Map<Integer, MetricInfo> taskMetrics = topologyMetric.get_taskMetric();
        
        Map<Integer, String> taskToComponent = null;
		try {
			taskToComponent = Cluster.get_all_task_component(stormClusterState, topologyId, null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("Failed to get taskToComponent");
            return ;
		}
        if (taskToComponent == null) {
            LOG.error("Failed to get taskToComponent");
            return ;
        }
        
        Map<String, MetricInfo> componentMetrics = topologyMetric.get_componentMetric();
        if (componentMetrics == null) {
            componentMetrics = new HashMap<String, MetricInfo>();
            topologyMetric.set_componentMetric(componentMetrics);
        }
        
        Map<String, AtomicInteger> componentTaskParallel = new HashMap<String, AtomicInteger>();
        Map<String, Map<String, AtomicInteger>> componentStreamParallel = new HashMap<String, Map<String, AtomicInteger>>();
        
        for (Entry<Integer, MetricInfo> entry : taskMetrics.entrySet()) {
            Integer taskId = entry.getKey();
            MetricInfo taskMetric = entry.getValue();
            
            String component = taskToComponent.get(taskId);
            if (component == null) {
                LOG.error("Failed to get component of task " + taskId);
                continue;
            }
            
            MetricInfo componentMetric = componentMetrics.get(component);
            
            componentMetric = mergeMetricInfo(taskMetric, componentMetric, MetricDef.MERGE_SUM_TAG);
            componentMetric = mergeMetricInfo(taskMetric, componentMetric, MetricDef.MERGE_AVG_TAG);
            
            Map<String, Map<String, MetricWindow>> input = mergeTaskStreams(componentMetric.get_inputMetric(), taskMetric.get_inputMetric(), componentStreamParallel);
            componentMetric.set_inputMetric(input);
            
            Map<String, Map<String, MetricWindow>> output = mergeTaskStreams(componentMetric.get_outputMetric(), taskMetric.get_outputMetric(), componentStreamParallel);
            componentMetric.set_outputMetric(output);
            
            componentMetrics.put(component, componentMetric);
            
            AtomicInteger counter = componentTaskParallel.get(component);
            if (counter == null) {
                counter = new AtomicInteger(0);
                componentTaskParallel.put(component, counter);
            }
            
            counter.incrementAndGet();
        }
        
        for (Entry<String, MetricInfo> entry : componentMetrics.entrySet()) {
            String componentName = entry.getKey();
            MetricInfo metricInfo = entry.getValue();
            
            AtomicInteger counter = componentTaskParallel.get(componentName);
            
            for (String tag : MetricDef.MERGE_AVG_TAG) {
                MetricWindow metricWindow = metricInfo.get_baseMetric().get(tag);
                
                avgMetricWindow(metricWindow, counter.get());
                
                avgStreams(metricInfo.get_inputMetric(), componentStreamParallel, tag);
                avgStreams(metricInfo.get_outputMetric(), componentStreamParallel, tag);
            }
        }
    }
    
    public void mergeComponent(TopologyMetric topologyMetric) {
        MetricInfo topologyMetricInfo = MetricThrift.mkMetricInfo();
        topologyMetric.set_topologyMetric(topologyMetricInfo);
        Map<String, MetricInfo> componentMetrics = topologyMetric.get_componentMetric();
        if (componentMetrics == null) {
            return;
        }
        
        for (MetricInfo componentMetric : componentMetrics.values()) {
            topologyMetricInfo = mergeMetricInfo(componentMetric, topologyMetricInfo, MetricDef.MERGE_SUM_TAG);
        }
        
        topologyMetric.set_topologyMetric(topologyMetricInfo);
    }
    
    public void mergeTopology(TopologyMetric topologyMetric, WorkerUploadMetrics workerMetrics) {
        String topologyId = workerMetrics.get_topology_id();
        
        Map<Integer, MetricInfo> taskMetrics = topologyMetric.get_taskMetric();
        if (taskMetrics == null) {
            taskMetrics = new HashMap<Integer, MetricInfo>();
            topologyMetric.set_taskMetric(taskMetrics);
        }
        taskMetrics.putAll(workerMetrics.get_taskMetric());
        
        String hostname = getWorkerHostname(workerMetrics);
        topologyMetric.put_to_workerMetric(getWorkerSlotName(hostname, workerMetrics.get_port()), workerMetrics.get_workerMetric());
        
    }
    
    public void mergeNetty(WorkerUploadMetrics workerMetric, String topologyId, Set<String> connections) {
        
    	if (topologyNettyMgr.getTopology(topologyId) == false) {
            return ;
        }
        Map<String, MetricInfo> connectionMetrics = workerMetric.get_nettyMetric().get_connections();
        for (Entry<String, MetricInfo> entry : connectionMetrics.entrySet()) {
            String connectionName = entry.getKey();
            MetricInfo metric = entry.getValue();
            
            MetricInfo cacheMetric = (MetricInfo)dbCache.get(getNettyConnectionKey(topologyId, connectionName));
            cacheMetric = MetricThrift.mergeMetricInfo(metric, cacheMetric);
            
            connections.add(connectionName);
            
            dbCache.put(getNettyConnectionKey(topologyId, connectionName), cacheMetric);
        }
    }
    
    public void mergeNetty(String topologyId, Set<String> connections) {
    	if (topologyNettyMgr.getTopology(topologyId) == false) {
    		LOG.info("Skip merge netty detail metrics");
            return ;
        }
        // @@@
        // this function will cost much memory when worker number is more than 200
        Map<String, MetricInfo> metricMap = new TreeMap<String, MetricInfo>();
        
        for (String connection : connections) {
            MetricInfo cacheMetric = (MetricInfo)dbCache.get(getNettyConnectionKey(topologyId, connection));
            if (cacheMetric == null) {
                LOG.warn("Failed to get cacheMetric of {}:{}", topologyId, connection );
                continue;
            }
            
            metricMap.put(connection, cacheMetric);
            dbCache.remove(getNettyConnectionKey(topologyId, connection));
        }
        
        dbCache.put(getNettyTopologyKey(topologyId), metricMap);
        // accelerate free memory
        metricMap.clear();
    }
    
    public void render() {
        for (Entry<String, Set<String>> entry : topologyWorkers.entrySet()) {
            String topologyId = entry.getKey();
            Set<String> workers = entry.getValue();
            Set<String> connections = new TreeSet<String>();
            
            TopologyMetric topologyMetric = new TopologyMetric();
            
            boolean isExistWorker = false;
            for (String workerId : workers) {
                WorkerUploadMetrics workerMetric = (WorkerUploadMetrics) dbCache.get(getWorkerKey(topologyId, workerId));
                if (workerMetric == null) {
                    LOG.warn("Failed to get WorkerUploadMetrics of " + getWorkerKey(topologyId, workerId));
                    continue;
                }
                isExistWorker = true;
                mergeTopology(topologyMetric, workerMetric);
                
                mergeNetty(workerMetric, topologyId, connections);
            }
            if (isExistWorker == false) {
            	LOG.info("No worker metrics of {}", topologyId);
            	continue;
            }
            
            mergeTasks(topologyMetric, topologyId);
            
            mergeComponent(topologyMetric);
            
            
            dbCache.put(getTopologyKey(topologyId), topologyMetric);
            
            mergeNetty(topologyId, connections);
            
            LOG.info("Successfully render topologyId of " + topologyId);
            
            uploadToAlimonitor(topologyMetric, topologyId);
            
            cleanDeadSupervisorWorker(topologyMetric);
            
            
            try {
                
                //LOG.info(topologyId + " metrics is :\n" + Utils.toPrettyJsonString(topologyMetric));
                LOG.info(topologyId + " finish metric");
                stormClusterState.set_topology_metric(topologyId, topologyMetric);
                LOG.info("Successfully uploaded toplogy metrics: " + topologyId);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                LOG.info("Failed to upload toplogy metrics: " + topologyId, e);
                continue;
            }
            
        }
    }
    
    public void handleUpdateEvent(Update event) {
        long start = System.currentTimeMillis();
        
        WorkerUploadMetrics workerMetrics = event.workerMetrics;
        
        String topologyId = workerMetrics.get_topology_id();
        if (removing.containsKey(topologyId) == true) {
            LOG.info("Topology " + topologyId + " has been removed, skip update");
            return;
        }
        
        Set<String> workers = topologyWorkers.get(topologyId);
        if (workers == null) {
            workers = new HashSet<String>();
            topologyWorkers.put(topologyId, workers);
        }
        
        String workerSlot = getWorkerSlotName(workerMetrics.get_supervisor_id(), workerMetrics.get_port());
        
        workers.add(workerSlot);
        dbCache.put(getWorkerKey(topologyId, workerSlot), workerMetrics);
        
        long end = System.currentTimeMillis();
        
        updateHistogram.update((end - start));
    }
    
    public void uploadToAlimonitor(TopologyMetric topologyMetric, String topologyId) {
        // @@@ TODO
    }
    
    
    public TopologyMetric getTopologyMetric(String topologyId) {
        long start = System.nanoTime();
        try {
            TopologyMetric ret = (TopologyMetric) dbCache.get(getTopologyKey(topologyId));
            if (ret == null) {
                return emptyTopologyMetric;
            } else {
                return ret;
            }
        }finally {
            long end = System.nanoTime();
            
            SimpleJStormMetric.updateHistorgram("getTopologyMetric", (end - start)/1000000.0d);
        }
    }
    
    public SortedMap<String, MetricInfo> getNettyMetric(String topologyId) {
        TreeMap<String, MetricInfo> ret = (TreeMap<String, MetricInfo>)dbCache.get(getNettyTopologyKey(topologyId));
        if (ret == null) {
            return emptyNettyMetric;
        }else {
            return ret;
        }
    }
    
    public static String getWorkerSlotName(String hostname, Integer port) {
        return hostname + ":" + port;
    }
    
    public static String getWorkerKey(String topologyId, String workerSlot) {
        return CACHE_NAMESPACE_METRIC + "@" + topologyId + "@" + workerSlot;
    }
    
    public static String getTopologyKey(String topologyId) {
        return CACHE_NAMESPACE_METRIC + "@" + topologyId;
    }
    
    public static String getNettyConnectionKey(String topologyId, String connection) {
        return CACHE_NAMESPACE_NETTY + "@" + topologyId + "@" + connection;
    }
    
    public static String getNettyTopologyKey(String topologyId) {
        return CACHE_NAMESPACE_NETTY + "@" + topologyId;
    }
    
    
}
