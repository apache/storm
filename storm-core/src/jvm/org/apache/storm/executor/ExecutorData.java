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
package org.apache.storm.executor;

import clojure.lang.IFn;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.storm.Config;
import org.apache.storm.cluster.*;
import org.apache.storm.daemon.GrouperFactory;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.executor.error.IReportError;
import org.apache.storm.executor.error.ReportError;
import org.apache.storm.executor.error.ReportErrorAndDie;
import org.apache.storm.generated.*;
import org.apache.storm.grouping.LoadAwareCustomStreamGrouping;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.stats.SpoutExecutorStats;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONValue;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorData {

    private final Map workerData;
    private final WorkerTopologyContext workerTopologyContext;
    private final List<Long> executorId;
    private final List<Integer> taskIds;
    private final String componentId;
    private final AtomicBoolean openOrPrepareWasCalled;
    private final Map stormConf;
    private final Map conf;
    private final DisruptorQueue receiveQueue;
    private final String stormId;
    private final HashMap sharedExecutorData;
    private final AtomicBoolean stormActive;
    private final AtomicReference<Map<String, DebugOptions>> stormComponentDebug;
    private final DisruptorQueue batchTransferWorkerQueue;
    private final Runnable suicideFn;
    private final IStormClusterState stormClusterState;
    private final Map<Integer, String> taskToComponent;
    private CommonStats stats;
    private final Map<Integer, Map<Integer, Map<String, IMetric>>> intervalToTaskToMetricToRegistry;
    private final Map<String, Map<String, LoadAwareCustomStreamGrouping>> streamToComponentToGrouper;
    private final IReportError reportError;
    private final ReportErrorAndDie reportErrorDie;
    private final Callable<Boolean> sampler;
    private final AtomicBoolean backpressure;
    private final ExecutorTransfer executorTransfer;
    private final String type;
    private final AtomicBoolean throttleOn;
    private final boolean isDebug;

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorData.class);

    public ExecutorData(Map<String, Object> workerData, List<Long> executorId) {
        this.workerData = workerData;
        this.workerTopologyContext = StormCommon.makeWorkerContext(workerData);
        this.executorId = executorId;
        this.taskIds = StormCommon.executorIdToTasks(executorId);
        this.componentId = workerTopologyContext.getComponentId(taskIds.get(0));
        this.openOrPrepareWasCalled = new AtomicBoolean(false);
        this.stormConf = normalizedComponentConf((Map) workerData.get("storm-conf"), workerTopologyContext, componentId);
        this.receiveQueue = (DisruptorQueue) (((Map) workerData.get("executor-receive-queue-map")).get(executorId));
        this.stormId = (String) workerData.get("storm-id");
        this.conf = (Map) workerData.get("conf");
        this.sharedExecutorData = new HashMap();
        this.stormActive = (AtomicBoolean) workerData.get("storm-active-atom");
        this.stormComponentDebug = (AtomicReference<Map<String, DebugOptions>>) workerData.get("storm-component->debug-atom");

        int sendSize = Utils.getInt(stormConf.get(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE));
        int waitTimeOutMs = Utils.getInt(stormConf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS));
        int batchSize = Utils.getInt(stormConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE));
        int batchTimeOutMs = Utils.getInt(stormConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS));
        this.batchTransferWorkerQueue = new DisruptorQueue(
                "executor" + executorId + "-send-queue", ProducerType.SINGLE, sendSize, waitTimeOutMs, batchSize, batchTimeOutMs);
        this.executorTransfer = new ExecutorTransfer(workerTopologyContext, batchTransferWorkerQueue, stormConf,
                (IFn) workerData.get("transfer-fn"));

        this.suicideFn = (Runnable) workerData.get("suicide-fn");
        try {
            this.stormClusterState = ClusterUtils.mkStormClusterState(workerData.get("state-store"), Utils.getWorkerACL(stormConf),
                    new ClusterStateContext(DaemonType.SUPERVISOR));
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }

        StormTopology topology = workerTopologyContext.getRawTopology();
        Map<String, SpoutSpec> spouts = topology.get_spouts();
        Map<String, Bolt> bolts = topology.get_bolts();
        if (spouts.containsKey(componentId)) {
            this.type = StatsUtil.SPOUT;
            this.stats = new SpoutExecutorStats(ConfigUtils.samplingRate(stormConf));
        } else if (bolts.containsKey(componentId)) {
            this.type = StatsUtil.BOLT;
            this.stats = new BoltExecutorStats(ConfigUtils.samplingRate(stormConf));
        } else {
            throw new RuntimeException("Could not find " + componentId + " in " + topology);
        }

        this.intervalToTaskToMetricToRegistry = new HashMap<>();
        this.taskToComponent = (Map<Integer, String>) workerData.get("task->component");
        this.streamToComponentToGrouper = outboundComponents(workerTopologyContext, componentId, stormConf);
        this.reportError = new ReportError(stormConf, stormClusterState, stormId, componentId, workerTopologyContext);
        this.reportErrorDie = new ReportErrorAndDie(reportError, suicideFn);
        this.sampler = ConfigUtils.mkStatsSampler(stormConf);
        this.backpressure = new AtomicBoolean(false);
        this.throttleOn = (AtomicBoolean) workerData.get("throttle-on");
        this.isDebug = Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
    }

    /**
     * Returns map of stream id to component id to grouper
     */
    private Map<String, Map<String, LoadAwareCustomStreamGrouping>> outboundComponents(
            WorkerTopologyContext workerTopologyContext, String componentId, Map stormConf) {
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
                        workerTopologyContext, componentId, streamId, outFields, grouping, outTasks, stormConf);
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

    private Map normalizedComponentConf(Map stormConf, WorkerTopologyContext topologyContext, String componentId) {
        List<Object> keysToRemove = ConfigUtils.All_CONFIGS();
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
        keysToRemove.remove(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_FIELD_NAME);
        keysToRemove.remove(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS);
        keysToRemove.remove(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME);
        keysToRemove.remove(Config.TOPOLOGY_STATE_PROVIDER);
        keysToRemove.remove(Config.TOPOLOGY_STATE_PROVIDER_CONFIG);

        Map<Object, Object> componentConf;
        String specJsonConf = topologyContext.getComponentCommon(componentId).get_json_conf();
        if (specJsonConf != null) {
            componentConf = (Map<Object, Object>) JSONValue.parse(specJsonConf);
            for (Object p : keysToRemove) {
                componentConf.remove(p);
            }
        } else {
            componentConf = new HashMap<>();
        }

        Map<Object, Object> ret = new HashMap<>();
        ret.putAll(stormConf);
        ret.putAll(componentConf);

        return ret;
    }

    public String getComponentId() {
        return componentId;
    }

    public Map getStormConf() {
        return stormConf;
    }

    public String getStormId() {
        return stormId;
    }

    public IReportError getReportError() {
        return reportError;
    }

    public WorkerTopologyContext getWorkerTopologyContext() {
        return workerTopologyContext;
    }

    public Callable<Boolean> getSampler() {
        return sampler;
    }

    public DisruptorQueue getReceiveQueue() {
        return receiveQueue;
    }

    public DisruptorQueue getBatchTransferWorkerQueue() {
        return batchTransferWorkerQueue;
    }

    public ExecutorTransfer getExecutorTransfer() {
        return executorTransfer;
    }

    public AtomicReference<Map<String, DebugOptions>> getStormComponentDebug() {
        return stormComponentDebug;
    }

    public CommonStats getStats() {
        return stats;
    }

    public List<Integer> getTaskIds() {
        return taskIds;
    }

    public Map<Integer, Map<Integer, Map<String, IMetric>>> getIntervalToTaskToMetricToRegistry() {
        return intervalToTaskToMetricToRegistry;
    }

    public AtomicBoolean getStormActive() {
        return stormActive;
    }

    public Map getWorkerData() {
        return workerData;
    }

    public void setOpenOrPrepareWasCalled(Boolean openOrPrepareWasCalled) {
        this.openOrPrepareWasCalled.set(openOrPrepareWasCalled);
    }

    public AtomicBoolean getOpenOrPrepareWasCalled() {
        return openOrPrepareWasCalled;
    }

    public AtomicBoolean getBackpressure() {
        return backpressure;
    }

    public ReportErrorAndDie getReportErrorDie() {
        return reportErrorDie;
    }

    public List<Long> getExecutorId() {
        return executorId;
    }

    public IStormClusterState getStormClusterState() {
        return stormClusterState;
    }

    public Map<String, Map<String, LoadAwareCustomStreamGrouping>> getStreamToComponentToGrouper() {
        return streamToComponentToGrouper;
    }

    public String getType() {
        return type;
    }

    public HashMap getSharedExecutorData() {
        return sharedExecutorData;
    }

    public Map getConf() {
        return conf;
    }

    public Map<Integer, String> getTaskToComponent() {
        return taskToComponent;
    }

    public AtomicBoolean getThrottleOn() {
        return throttleOn;
    }

    public boolean isDebug() {
        return isDebug;
    }
}
