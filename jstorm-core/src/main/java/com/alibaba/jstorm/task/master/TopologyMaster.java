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
package com.alibaba.jstorm.task.master;

import backtype.storm.generated.*;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IDynamicComponent;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.NimbusClient;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.metric.TopologyMetricContext;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.backpressure.BackpressureCoordinator;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeatUpdater;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.google.common.collect.Maps;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Topology master is responsible for the process of general topology
 * information, e.g. task heartbeat update, metrics data update....
 *
 * @author Basti Liu
 */
public class TopologyMaster implements IBolt, IDynamicComponent  {

    private static final long serialVersionUID = 4690656768333833626L;

    private static final Logger LOG = getLogger(TopologyMaster.class);
    private final Logger metricLogger = getLogger(TopologyMetricContext.class);

    public static final int MAX_BATCH_SIZE = 10000;
    private final MetricInfo dummy = MetricUtils.mkMetricInfo();

    public static final String FIELD_METRIC_WORKER = "worker";
    public static final String FIELD_METRIC_METRICS = "metrics";
    public static final String FILED_HEARBEAT_EVENT = "hbEvent";
    public static final String FILED_CTRL_EVENT = "ctrlEvent";

    private Map conf;
    private StormClusterState zkCluster;
    private OutputCollector collector;

    private int taskId;
    private String topologyId;
    private volatile Set<ResourceWorkerSlot> workerSet;
    private IntervalCheck intervalCheck;

    private TaskHeartbeatUpdater taskHeartbeatUpdater;

    private BackpressureCoordinator backpressureCoordinator;

    private TopologyMetricContext topologyMetricContext;

    private ScheduledExecutorService uploadMetricsExecutor;

    private Thread updateThread;
    private BlockingQueue<Tuple> queue = new LinkedBlockingDeque<Tuple>();
    private IntervalCheck threadAliveCheck;

    private volatile boolean isActive = true;

    private class TopologyMasterRunnable implements Runnable {
        @Override
        public void run() {
            while (isActive) {
                try {
                    Tuple event = queue.take();
                    if (event != null) {
                        eventHandle(event);
                    }
                } catch (Throwable e) {
                    LOG.error("Failed to process event", e);
                }
            }
        }
        
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.conf = context.getStormConf();
        this.collector = collector;
        this.taskId = context.getThisTaskId();
        this.topologyId = context.getTopologyId();
        this.zkCluster = context.getZkCluster();

        try {
            Assignment assignment = zkCluster.assignment_info(topologyId, null);
            this.workerSet = assignment.getWorkers();
            intervalCheck = new IntervalCheck();
            intervalCheck.setInterval(10);
            intervalCheck.start();
        } catch (Exception e) {
            LOG.error("Failed to get assignment for " + topologyId);
        }

        this.taskHeartbeatUpdater = new TaskHeartbeatUpdater(this.conf, topologyId, taskId, zkCluster);

        this.backpressureCoordinator = new BackpressureCoordinator(collector, context, taskId);

        this.topologyMetricContext = new TopologyMetricContext(topologyId, this.workerSet, this.conf);

        this.uploadMetricsExecutor = Executors.newSingleThreadScheduledExecutor();
        this.uploadMetricsExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                int secOffset = TimeUtils.secOffset();
                int offset = 35;
                if (secOffset < offset) {
                    JStormUtils.sleepMs((offset - secOffset) * 1000);
                } else if (secOffset == offset) {
                    // do nothing
                } else {
                    JStormUtils.sleepMs((60 - secOffset + offset) * 1000);
                }
                if (topologyMetricContext.getUploadedWorkerNum() > 0) {
                    metricLogger.info("force upload metrics.");
                    mergeAndUpload();
                }
            }
        }, 5, 60, TimeUnit.SECONDS);

        updateThread = new Thread(new TopologyMasterRunnable());
        updateThread.start();

        threadAliveCheck = new IntervalCheck();
        threadAliveCheck.setInterval(30);
        threadAliveCheck.start();
    }

    @Override
    public void execute(Tuple input) {
        if (input != null) {
            
            try {
                queue.put(input);
            } catch (InterruptedException e) {
                LOG.error("Failed to put event to taskHb updater's queue", e);
            }
            
            if (threadAliveCheck.check()) {
                if (updateThread == null || updateThread.isAlive() == false) {
                    updateThread = new Thread(new TopologyMasterRunnable());
                    updateThread.start();
                }
            }

            collector.ack(input);
        } else {
            LOG.error("Received null tuple!");
        }
    }

    @Override
    public void cleanup() {
	    isActive = false;
        LOG.info("Successfully cleanup");
    }

    private void updateTopologyWorkerSet() {
        if (intervalCheck.check()) {
            Assignment assignment;
            try {
                assignment = zkCluster.assignment_info(topologyId, null);
                this.workerSet = assignment.getWorkers();
            } catch (Exception e) {
                LOG.error("Failed to get assignment for " + topologyId);
            }

        }
    }

    private void eventHandle(Tuple input) {
        updateTopologyWorkerSet();

        String stream = input.getSourceStreamId();

        try {
            if (stream.equals(Common.TOPOLOGY_MASTER_HB_STREAM_ID)) {
                taskHeartbeatUpdater.process(input);
            } else if (stream.equals(Common.TOPOLOGY_MASTER_METRICS_STREAM_ID)) {
                updateMetrics(input);
            } else if (stream.equals(Common.TOPOLOGY_MASTER_CONTROL_STREAM_ID)) {
                backpressureCoordinator.process(input);
            }
        } catch (Exception e) {
            LOG.error("Failed to handle event: " + input.toString(), e);
        }
    }

    @Override
    public void update(Map conf) {
        LOG.info("Topology master received new conf:" + conf);

        if (backpressureCoordinator.isBackpressureConfigChange(conf)) {
            backpressureCoordinator.updateBackpressureConfig(conf);
        }
    }

    private void updateMetrics(Tuple input) {
        String workerSlot = (String) input.getValueByField(FIELD_METRIC_WORKER);
        WorkerUploadMetrics metrics = (WorkerUploadMetrics) input.getValueByField(FIELD_METRIC_METRICS);
        topologyMetricContext.addToMemCache(workerSlot, metrics.get_allMetrics());
        metricLogger.info("received metrics from:{}, size:{}", workerSlot, metrics.get_allMetrics().get_metrics_size());

        if (topologyMetricContext.readyToUpload()) {
            metricLogger.info("all {} worker slots have updated metrics, start merging & uploading...",
                    topologyMetricContext.getWorkerNum());
            uploadMetricsExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    mergeAndUpload();
                }
            });
        }
    }

    private void mergeAndUpload() {
        // double check
        if (topologyMetricContext.getUploadedWorkerNum() > 0) {
            TopologyMetric tpMetric = topologyMetricContext.mergeMetrics();
            if (tpMetric != null) {
                uploadMetrics(tpMetric);
            }

            topologyMetricContext.resetUploadedMetrics();
            //MetricUtils.logMetrics(tpMetric.get_componentMetric());
        }
    }

    /**
     * upload metrics sequentially due to thrift frame size limit (15MB)
     */
    private void uploadMetrics(TopologyMetric tpMetric) {
        long start = System.currentTimeMillis();
        if (StormConfig.local_mode(conf)) {
            return;
        } else {
            NimbusClient client = null;
            try {
                client = NimbusClient.getConfiguredClient(conf);
                Nimbus.Client client1 = client.getClient();

                MetricInfo topologyMetrics = tpMetric.get_topologyMetric();
                MetricInfo componentMetrics = tpMetric.get_componentMetric();
                MetricInfo taskMetrics = tpMetric.get_taskMetric();
                MetricInfo streamMetrics = tpMetric.get_streamMetric();
                MetricInfo workerMetrics = tpMetric.get_workerMetric();
                MetricInfo nettyMetrics = tpMetric.get_nettyMetric();

                int totalSize = topologyMetrics.get_metrics_size() + componentMetrics.get_metrics_size() +
                        taskMetrics.get_metrics_size() + streamMetrics.get_metrics_size() +
                        workerMetrics.get_metrics_size() + nettyMetrics.get_metrics_size();

                // for small topologies, send all metrics together to ease the pressure of nimbus
                if (totalSize < MAX_BATCH_SIZE) {
                    client1.uploadTopologyMetrics(topologyId,
                            new TopologyMetric(topologyMetrics, componentMetrics, workerMetrics, taskMetrics,
                                    streamMetrics, nettyMetrics));
                } else {
                    client1.uploadTopologyMetrics(topologyId,
                            new TopologyMetric(topologyMetrics, componentMetrics, dummy, dummy, dummy, dummy));
                    batchUploadMetrics(client1, topologyId, workerMetrics, MetaType.WORKER);
                    batchUploadMetrics(client1, topologyId, taskMetrics, MetaType.TASK);
                    batchUploadMetrics(client1, topologyId, streamMetrics, MetaType.STREAM);
                    batchUploadMetrics(client1, topologyId, nettyMetrics, MetaType.NETTY);
                }
            } catch (Exception e) {
                LOG.error("Failed to upload worker metrics", e);
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        }
        metricLogger.info("upload metrics, cost:{}", System.currentTimeMillis() - start);
    }

    private void batchUploadMetrics(Nimbus.Client client, String topologyId, MetricInfo metricInfo, MetaType metaType) {
        if (metricInfo.get_metrics_size() > MAX_BATCH_SIZE) {
            Map<String, Map<Integer, MetricSnapshot>> data = metricInfo.get_metrics();

            Map<String, Map<Integer, MetricSnapshot>> part = Maps.newHashMapWithExpectedSize(MAX_BATCH_SIZE);
            MetricInfo uploadPart = new MetricInfo();
            int i = 0;
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> entry : data.entrySet()) {
                part.put(entry.getKey(), entry.getValue());
                if (++i >= MAX_BATCH_SIZE) {
                    uploadPart.set_metrics(part);
                    doUpload(client, topologyId, uploadPart, metaType);

                    i = 0;
                    part.clear();
                }
            }
            if (part.size() > 0) {
                uploadPart.set_metrics(part);
                doUpload(client, topologyId, uploadPart, metaType);
            }
        } else {
            doUpload(client, topologyId, metricInfo, metaType);
        }
    }

    private void doUpload(Nimbus.Client client, String topologyId, MetricInfo part, MetaType metaType) {
        try {
            if (metaType == MetaType.TASK) {
                client.uploadTopologyMetrics(topologyId,
                        new TopologyMetric(dummy, dummy, dummy, part, dummy, dummy));
            } else if (metaType == MetaType.STREAM) {
                client.uploadTopologyMetrics(topologyId,
                        new TopologyMetric(dummy, dummy, dummy, dummy, part, dummy));
            } else if (metaType == MetaType.WORKER) {
                client.uploadTopologyMetrics(topologyId,
                        new TopologyMetric(dummy, dummy, part, dummy, dummy, dummy));
            } else if (metaType == MetaType.NETTY) {
                client.uploadTopologyMetrics(topologyId,
                        new TopologyMetric(dummy, dummy, dummy, dummy, dummy, part));
            }
        } catch (Exception ex) {
            LOG.error("Error", ex);
        }
    }
}
