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

import com.lmax.disruptor.EventHandler;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.StormTimer;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.Task;
import org.apache.storm.executor.error.IReportError;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

public abstract class BaseExecutor implements Callable, EventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(BaseExecutor.class);

    protected final ExecutorData executorData;
    protected final Map stormConf;
    protected final String componentId;
    protected final WorkerTopologyContext workerTopologyContext;
    protected final IReportError reportError;
    protected final Callable<Boolean> sampler;
    protected final Random rand;
    protected final DisruptorQueue transferQueue;
    protected final DisruptorQueue receiveQueue;
    protected final Map<Integer, Task> idToTask;
    protected final Map<String, String> credentials;
    protected final Boolean isDebug;
    protected final Boolean isEventLoggers;
    protected String hostname;

    public BaseExecutor(ExecutorData executorData, Map<Integer, Task> idToTask, Map<String, String> credentials) {
        this.executorData = executorData;
        this.stormConf = executorData.getStormConf();
        this.componentId = executorData.getComponentId();
        this.workerTopologyContext = executorData.getWorkerTopologyContext();
        this.reportError = executorData.getReportError();
        this.sampler = executorData.getSampler();
        this.rand = new Random(Utils.secureRandomLong());
        this.transferQueue = executorData.getBatchTransferWorkerQueue();
        this.receiveQueue = executorData.getReceiveQueue();
        this.idToTask = idToTask;
        this.credentials = credentials;
        this.isDebug = Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
        this.isEventLoggers = StormCommon.hasEventLoggers(stormConf);

        try {
            this.hostname = Utils.hostname(stormConf);
        } catch (UnknownHostException ignored) {
            this.hostname = "";
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onEvent(Object event, long seq, boolean endOfBatch) throws Exception {
        ArrayList<AddressedTuple> addressedTuples = (ArrayList<AddressedTuple>) event;
        for (AddressedTuple addressedTuple : addressedTuples) {
            TupleImpl tuple = (TupleImpl) addressedTuple.getTuple();
            int taskId = addressedTuple.getDest();
            if (isDebug) {
                LOG.info("Processing received message FOR {} TUPLE: {}", taskId, tuple);
            }
            if (taskId != AddressedTuple.BROADCAST_DEST) {
                tupleActionFn(taskId, tuple);
            } else {
                for (Integer t : executorData.getTaskIds()) {
                    tupleActionFn(t, tuple);
                }
            }
        }
    }

    public void metricsTick(Task taskData, TupleImpl tuple) {
        try {
            Integer interval = tuple.getInteger(0);
            int taskId = taskData.getTaskId();
            Map<Integer, Map<String, IMetric>> taskToMetricToRegistry = executorData.getIntervalToTaskToMetricToRegistry().get(interval);
            Map<String, IMetric> nameToRegistry = null;
            if (taskToMetricToRegistry != null) {
                nameToRegistry = taskToMetricToRegistry.get(taskId);
            }
            if (nameToRegistry != null) {
                IMetricsConsumer.TaskInfo taskInfo = new IMetricsConsumer.TaskInfo(hostname, workerTopologyContext.getThisWorkerPort(),
                        componentId, taskId, (System.currentTimeMillis() / 1000), interval);
                List<IMetricsConsumer.DataPoint> dataPoints = new ArrayList<>();
                for (Map.Entry<String, IMetric> entry : nameToRegistry.entrySet()) {
                    IMetric metric = entry.getValue();
                    Object value = metric.getValueAndReset();
                    if (value != null) {
                        IMetricsConsumer.DataPoint dataPoint = new IMetricsConsumer.DataPoint(entry.getKey(), value);
                        dataPoints.add(dataPoint);
                    }
                }
                if (!dataPoints.isEmpty()) {
                    ExecutorCommon.sendUnanchored(taskData, Constants.METRICS_STREAM_ID, new Values(taskInfo, dataPoints), executorData.getExecutorTransfer());
                }
            }
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public abstract void tupleActionFn(int taskId, TupleImpl tuple) throws Exception;

    protected abstract void init();

    protected void setupMetrics() {
        Map<Integer, Map<Integer, Map<String, IMetric>>> intervalToTaskToMetricToRegistry = executorData.getIntervalToTaskToMetricToRegistry();
        for (final Integer interval : intervalToTaskToMetricToRegistry.keySet()) {
            StormTimer timerTask = (StormTimer) executorData.getWorkerData().get("user-timer");
            timerTask.scheduleRecurring(interval, interval, new Runnable() {
                @Override
                public void run() {
                    TupleImpl tuple =
                            new TupleImpl(workerTopologyContext, new Values(interval), (int) Constants.SYSTEM_TASK_ID, Constants.METRICS_TICK_STREAM_ID);
                    AddressedTuple addressedTuple = new AddressedTuple(AddressedTuple.BROADCAST_DEST, tuple);
                    receiveQueue.publish(addressedTuple);
                }
            });
        }
    }
}
