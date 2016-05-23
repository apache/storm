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

import com.google.common.collect.Lists;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.StormTimer;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.Task;
import org.apache.storm.executor.bolt.BoltExecutor;
import org.apache.storm.executor.spout.SpoutExecutor;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DisruptorBackpressureCallback;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.WorkerBackpressureThread;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Executor {
    private final Logger LOG = LoggerFactory.getLogger(Executor.class);

    private final Map workerData;
    private final List<Long> executorId;
    private final Map<String, String> credentials;
    private final ExecutorData executorData;
    private final String componentId;
    private final Map stormConf;

    private Executor(Map<String, Object> workerData, List<Long> executorId, Map<String, String> credentials) {
        this.workerData = workerData;
        this.executorId = executorId;
        this.credentials = credentials;
        this.executorData = new ExecutorData(workerData, executorId);
        this.componentId = executorData.getComponentId();
        this.stormConf = executorData.getStormConf();
        LOG.info("Loading executor " + componentId + ":" + executorId);
    }

    public static ExecutorShutdown mkExecutor(Map workerData, List<Long> executorId, Map<String, String> credentials) throws Exception {
        Map<String, Object> convertedWorkerData = Utils.convertMap(workerData);
        Executor executor = new Executor(convertedWorkerData, executorId, credentials);
        return executor.execute();
    }

    private ExecutorShutdown execute() throws Exception {
        Map<Integer, Task> idToTask = new HashMap<>();
        for (Integer taskId : executorData.getTaskIds()) {
            Task task = new Task(executorData, taskId);
            ExecutorCommon.sendUnanchored(task, StormCommon.SYSTEM_STREAM_ID, new Values("startup"), executorData.getExecutorTransfer());
            idToTask.put(taskId, task);
        }
        LOG.info("Loading executor tasks " + componentId + ":" + executorId);

        registerBackpressure();
        Utils.SmartThread systemThreads =
                Utils.asyncLoop(executorData.getExecutorTransfer(), executorData.getExecutorTransfer().getName(), executorData.getReportErrorDie());

        BaseExecutor baseExecutor;
        String type = executorData.getType();
        if (StatsUtil.SPOUT.equals(type)) {
            baseExecutor = new SpoutExecutor(executorData, idToTask, credentials);
        } else if (StatsUtil.BOLT.equals(type)) {
            baseExecutor = new BoltExecutor(executorData, idToTask, credentials);
        } else {
            throw new RuntimeException("Could not find  " + componentId + " in topology");
        }

        String handlerName = componentId + "-executor" + executorId;
        Utils.SmartThread handlers = Utils.asyncLoop(baseExecutor, false, executorData.getReportErrorDie(), Thread.NORM_PRIORITY, false, true, handlerName);
        setupTicks(StatsUtil.SPOUT.equals(type));
        LOG.info("Finished loading executor " + componentId + ":" + executorId);
        return new ExecutorShutdown(executorData, Lists.newArrayList(systemThreads, handlers), idToTask);
    }

    private void registerBackpressure() {
        DisruptorQueue receiveQueue = executorData.getReceiveQueue();
        receiveQueue.registerBackpressureCallback(new DisruptorBackpressureCallback() {
            @Override
            public void highWaterMark() throws Exception {
                AtomicBoolean enablePressure = executorData.getBackpressure();
                if (!enablePressure.get()) {
                    enablePressure.set(true);
                    LOG.debug("executor " + executorId + " is congested, set backpressure flag true");
                    WorkerBackpressureThread.notifyBackpressureChecker(workerData.get("backpressure-trigger"));
                }
            }

            @Override
            public void lowWaterMark() throws Exception {
                AtomicBoolean enablePressure = executorData.getBackpressure();
                if (enablePressure.get()) {
                    enablePressure.set(false);
                    LOG.debug("executor " + executorId + " is not-congested, set backpressure flag false");
                    WorkerBackpressureThread.notifyBackpressureChecker(workerData.get("backpressure-trigger"));
                }
            }
        });
        receiveQueue.setHighWaterMark(Utils.getDouble(stormConf.get(Config.BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK)));
        receiveQueue.setLowWaterMark(Utils.getDouble(stormConf.get(Config.BACKPRESSURE_DISRUPTOR_LOW_WATERMARK)));
        receiveQueue.setEnableBackpressure(Utils.getBoolean(stormConf.get(Config.TOPOLOGY_BACKPRESSURE_ENABLE), false));
    }

    protected void setupTicks(boolean isSpout) {
        final Integer tickTimeSecs = Utils.getInt(stormConf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS), null);
        boolean enableMessageTimeout = (Boolean) stormConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS);
        if (tickTimeSecs != null) {
            if (Utils.isSystemId(componentId) || (!enableMessageTimeout && isSpout)) {
                LOG.info("Timeouts disabled for executor " + componentId + ":" + executorId);
                StormTimer timerTask = (StormTimer) workerData.get("user-timer");
                timerTask.scheduleRecurring(tickTimeSecs, tickTimeSecs, new Runnable() {
                    @Override
                    public void run() {
                        TupleImpl tuple = new TupleImpl(executorData.getWorkerTopologyContext(), new Values(tickTimeSecs), (int) Constants.SYSTEM_TASK_ID,
                                Constants.METRICS_TICK_STREAM_ID);
                        List<AddressedTuple> metricTickTuple = Lists.newArrayList(new AddressedTuple(AddressedTuple.BROADCAST_DEST, tuple));
                        executorData.getReceiveQueue().publish(metricTickTuple);
                    }
                });
            }
        }
    }

}
