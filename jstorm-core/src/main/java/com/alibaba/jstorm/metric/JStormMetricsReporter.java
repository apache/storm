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
package com.alibaba.jstorm.metric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricWindow;
import backtype.storm.generated.NettyMetric;
import backtype.storm.generated.WorkerUploadMetrics;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.common.metric.Gauge;
import com.alibaba.jstorm.common.metric.MetricFilter;
import com.alibaba.jstorm.common.metric.MetricRegistry;
import com.alibaba.jstorm.common.metric.window.Metric;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.utils.JStormUtils;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;

public class JStormMetricsReporter extends RunnableCallback {
    private static final Logger LOG = LoggerFactory
            .getLogger(JStormMetricsReporter.class);

    private MetricRegistry workerMetrics = JStormMetrics.workerMetrics;
    private Map<Integer, MetricRegistry> taskMetrics =
            JStormMetrics.taskMetrics;
    private MetricRegistry skipMetrics = JStormMetrics.skipMetrics;

    private JStormMetricFilter inputFilter;

    private JStormMetricFilter outputFilter;

    private Map conf;
    private String topologyId;
    private String supervisorId;
    private int port;
    private int frequence;

    private StormClusterState clusterState;
    private boolean localMode = false;
    private NimbusClient client;

    public JStormMetricsReporter(WorkerData workerData) {
        this.conf = workerData.getStormConf();
        this.topologyId = (String) conf.get(Config.TOPOLOGY_ID);
        this.supervisorId = workerData.getSupervisorId();
        this.port = workerData.getPort();
        this.frequence = ConfigExtension.getWorkerMetricReportFrequency(conf);
        this.clusterState = workerData.getZkCluster();

        outputFilter = new JStormMetricFilter(MetricDef.OUTPUT_TAG);
        inputFilter = new JStormMetricFilter(MetricDef.INPUT_TAG);
        localMode = StormConfig.local_mode(conf);
        LOG.info("Successfully start ");
    }

    protected boolean getMoreMetric(
            Map<String, Map<String, MetricWindow>> extraMap,
            JStormMetricFilter metricFilter, String metricFullName,
            Map<Integer, Double> metricWindow) {
        if (metricFilter.matches(metricFullName, null) == false) {
            return false;
        }

        int pos = metricFullName.indexOf(MetricRegistry.NAME_SEPERATOR);
        if (pos <= 0 || pos >= metricFullName.length() - 1) {
            return false;
        }

        String metricName = metricFullName.substring(0, pos);
        String extraName = metricFullName.substring(pos + 1);

        Map<String, MetricWindow> item = extraMap.get(metricName);
        if (item == null) {
            item = new HashMap<String, MetricWindow>();
            extraMap.put(metricName, item);
        }

        MetricWindow metricWindowThrift = new MetricWindow();
        metricWindowThrift.set_metricWindow(metricWindow);

        item.put(extraName, metricWindowThrift);

        return true;
    }
    
    protected void insertNettyMetrics(Map<String, MetricInfo> nettyMetricInfo, 
                    Map<Integer, Double> snapshot,
                    String metricFullName) {
        int pos = metricFullName.indexOf(MetricRegistry.NAME_SEPERATOR);
        if (pos < 0 || pos >= metricFullName.length() - 1) {
            return ;
        }
        
        String realHeader = metricFullName.substring(0, pos);
        String nettyConnection = metricFullName.substring(pos + 1);
        
        MetricInfo metricInfo = nettyMetricInfo.get(nettyConnection);
        if (metricInfo == null) {
            metricInfo = MetricThrift.mkMetricInfo();
            
            nettyMetricInfo.put(nettyConnection, metricInfo);
        }
        
        MetricThrift.insert(metricInfo, realHeader, snapshot);
    }
    
    protected void insertMergeList(Map<String, List<Map<Integer, Double> > > mergeMap, 
                    List<String> mergeList, 
                    Map<Integer, Double> snapshot,
                    String name) {
        for (String tag : mergeList) {
            if (name.startsWith(tag) == false) {
                continue;
            }
            List<Map<Integer, Double> > list = mergeMap.get(tag);
            if (list == null) {
                list = new ArrayList<Map<Integer,Double>>();
                mergeMap.put(tag, list);
            }
            
            list.add(snapshot);
            
        }
    }
    
    protected void doMergeList(MetricInfo workerMetricInfo, 
                    Map<String, List<Map<Integer, Double> > > mergeMap) {
        for (Entry<String, List<Map<Integer, Double> > > entry : mergeMap.entrySet()) {
            String name = entry.getKey();
            List<Map<Integer, Double>> list = entry.getValue();
            
            Map<Integer, Double> merged = JStormUtils.mergeMapList(list);
            
            MetricThrift.insert(workerMetricInfo, name, merged);
        }
    }

    public MetricInfo computWorkerMetrics() {
        MetricInfo workerMetricInfo = MetricThrift.mkMetricInfo();
        Map<String, MetricInfo> nettyMetricInfo = new HashMap<String, MetricInfo>();

        Map<String, List<Map<Integer, Double> > > mergeMap = 
                new HashMap<String, List<Map<Integer,Double> > >();
        List<String> mergeList = new ArrayList<String>();
        mergeList.add(MetricDef.NETTY_CLI_SEND_SPEED);
        
        Map<String, Metric> workerMetricMap = workerMetrics.getMetrics();
        for (Entry<String, Metric> entry : workerMetricMap.entrySet()) {
            String name = entry.getKey();
            Map<Integer, Double> snapshot = entry.getValue().getSnapshot();

            if (MetricDef.isNettyDetails(name) == false) {
                MetricThrift.insert(workerMetricInfo, name, snapshot);
                continue;
            }
            
            insertNettyMetrics(nettyMetricInfo, snapshot, name);
            
            insertMergeList(mergeMap, mergeList, snapshot, name);
            
        }
        
        doMergeList(workerMetricInfo, mergeMap);
        
        JStormMetrics.setExposeWorkerMetrics(workerMetricInfo);
        JStormMetrics.setExposeNettyMetrics(nettyMetricInfo);
        return workerMetricInfo;
    }

    public boolean isTaskQueueFull(Metric metric,
            Map<Integer, Double> snapshot, String name) {
        if (metric instanceof Gauge) {
            if (MetricDef.TASK_QUEUE_SET.contains(name)) {
                for (Entry<Integer, Double> entry : snapshot.entrySet()) {
                    if (entry.getValue() == MetricDef.FULL_RATIO) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public Map<Integer, MetricInfo> computeTaskMetrics() {
        Map<Integer, MetricInfo> ret = new HashMap<Integer, MetricInfo>();

        for (Entry<Integer, MetricRegistry> entry : taskMetrics.entrySet()) {
            Integer taskId = entry.getKey();
            MetricRegistry taskMetrics = entry.getValue();

            Map<String, Map<String, MetricWindow>> inputMap =
                    new HashMap<String, Map<String, MetricWindow>>();
            Map<String, Map<String, MetricWindow>> outputMap =
                    new HashMap<String, Map<String, MetricWindow>>();

            MetricInfo taskMetricInfo = MetricThrift.mkMetricInfo();
            taskMetricInfo.set_inputMetric(inputMap);
            taskMetricInfo.set_outputMetric(outputMap);
            ret.put(taskId, taskMetricInfo);

            for (Entry<String, Metric> metricEntry : taskMetrics.getMetrics()
                    .entrySet()) {
                String name = metricEntry.getKey();
                Metric metric = metricEntry.getValue();
                Map<Integer, Double> snapshot = metric.getSnapshot();

                boolean isInput =
                        getMoreMetric(inputMap, inputFilter, name, snapshot);
                boolean isOutput =
                        getMoreMetric(outputMap, outputFilter, name, snapshot);

                if (isInput == false && isOutput == false) {
                    MetricThrift.insert(taskMetricInfo, name, snapshot);
                }
            }

            MetricThrift.merge(taskMetricInfo, inputMap);
            MetricThrift.merge(taskMetricInfo, outputMap);

        }

        JStormMetrics.setExposeTaskMetrics(ret);
        return ret;
    }
    
    public void healthCheck(Integer taskId, HealthCheckRegistry healthCheck) {
    	if (taskId == null) {
    		return ;
    	}
    	
    	final Map<String, HealthCheck.Result> results =
    			healthCheck.runHealthChecks();
        for (Entry<String, HealthCheck.Result> resultEntry : results
                .entrySet()) {
            HealthCheck.Result result = resultEntry.getValue();
            if (result.isHealthy() == false) {
                LOG.warn("{}:{}", taskId, result.getMessage());
                try {
                    clusterState.report_task_error(topologyId, taskId,
                            result.getMessage());
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    LOG.error(e.getMessage(), e);
                }

            }
        }
    }

    public void healthCheck() {
        Integer firstTask = null;

        Map<Integer, HealthCheckRegistry> taskHealthCheckMap =
                JStormHealthCheck.getTaskhealthcheckmap();

        for (Entry<Integer, HealthCheckRegistry> entry : taskHealthCheckMap
                .entrySet()) {
            Integer taskId = entry.getKey();
            HealthCheckRegistry taskHealthCheck = entry.getValue();

            healthCheck(taskId, taskHealthCheck);

            if (firstTask != null) {
                firstTask = taskId;
            }
        }

        HealthCheckRegistry workerHealthCheck =
                JStormHealthCheck.getWorkerhealthcheck();
        healthCheck(firstTask, workerHealthCheck);
        

    }

    @Override
    public void run() {

        try {
            // TODO Auto-generated method stub
            MetricInfo workerMetricInfo = computWorkerMetrics();
            
            Map<Integer, MetricInfo> taskMetricMap = computeTaskMetrics();

            WorkerUploadMetrics upload = new WorkerUploadMetrics();
            upload.set_topology_id(topologyId);
            upload.set_supervisor_id(supervisorId);
            upload.set_port(port);
            upload.set_workerMetric(workerMetricInfo);
            upload.set_nettyMetric(
                            new NettyMetric(
                                   JStormMetrics.getExposeNettyMetrics(), 
                                   JStormMetrics.getExposeNettyMetrics().size()));
            upload.set_taskMetric(taskMetricMap);
            
            uploadMetric(upload);
            
            healthCheck();

            LOG.info("Successfully upload worker's metrics");
            LOG.info(Utils.toPrettyJsonString(workerMetricInfo));
            LOG.info(Utils.toPrettyJsonString(JStormMetrics.getExposeNettyMetrics()));
            LOG.info(Utils.toPrettyJsonString(taskMetricMap));
        } catch (Exception e) {
            LOG.error("Failed to upload worker metrics", e);
        }

    }

    public void uploadMetric(WorkerUploadMetrics upload) {
        if (StormConfig.local_mode(conf)) {
            try {
                byte[] temp = Utils.serialize(upload);

                LocalCluster.getInstance().getLocalClusterMap().getNimbus()
                        .workerUploadMetric(upload);
            } catch (TException e) {
                // TODO Auto-generated catch block
                LOG.error("Failed to upload worker metrics", e);
            }
        } else {
            try {
            	if (client == null) {
            		client = NimbusClient.getConfiguredClient(conf);
            	}
                client.getClient().workerUploadMetric(upload);
            } catch (Exception e) {
                LOG.error("Failed to upload worker metrics", e);
                if (client != null) {
                    client.close();
                    client = null;
                }
            } finally {
                
            }
        }
    }

    @Override
    public Object getResult() {
        return frequence;
    }
    
    @Override
    public void shutdown() {
    	if (client != null) {
            client.close();
            client = null;
        }
    }

    public static class JStormMetricFilter implements MetricFilter {
        private static final long serialVersionUID = -8886536175626248855L;
        private String[] tags;

        public JStormMetricFilter(String[] tags) {
            this.tags = tags;
        }

        @Override
        public boolean matches(String name, Metric metric) {
            // TODO Auto-generated method stub
            for (String tag : tags) {
                if (name.startsWith(tag)) {
                    return true;
                }
            }
            return false;
        }

    }

}
