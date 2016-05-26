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
package org.apache.storm.executor.spout;

import com.google.common.collect.ImmutableMap;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.metrics.BuiltinMetricsUtil;
import org.apache.storm.daemon.metrics.SpoutThrottlingMetrics;
import org.apache.storm.executor.BaseExecutor;
import org.apache.storm.executor.ExecutorCommon;
import org.apache.storm.executor.ExecutorData;
import org.apache.storm.executor.TupleInfo;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.ISpoutWaitStrategy;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SpoutExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SpoutExecutor.class);

    private final ISpoutWaitStrategy spoutWaitStrategy;
    private final Integer maxSpoutPending;
    private final AtomicBoolean lastActive;
    private final List<ISpout> spouts;
    private final List<SpoutOutputCollector> outputCollectors;
    private final MutableLong emittedCount;
    private final MutableLong emptyEmitStreak;
    private final SpoutThrottlingMetrics spoutThrottlingMetrics;
    private final boolean hasAckers;
    private final RotatingMap<Long, TupleInfo> pending;
    private final boolean backPressureEnabled;
    private final AtomicBoolean throttleOn;

    public SpoutExecutor(final ExecutorData executorData, final Map<Integer, Task> idToTask, Map<String, String> credentials) {
        super(executorData, idToTask, credentials);
        this.spoutWaitStrategy = Utils.newInstance((String) stormConf.get(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY));
        this.spoutWaitStrategy.prepare(stormConf);

        this.maxSpoutPending = Utils.getInt(stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), 0) * idToTask.size();
        this.backPressureEnabled = Utils.getBoolean(stormConf.get(Config.TOPOLOGY_BACKPRESSURE_ENABLE), false);
        this.throttleOn = executorData.getThrottleOn();

        this.lastActive = new AtomicBoolean(false);
        this.spouts = new ArrayList<>();
        for (Task task : idToTask.values()) {
            this.spouts.add((ISpout) task.getTaskObject());
        }
        this.hasAckers = StormCommon.hasAckers(stormConf);
        this.emittedCount = new MutableLong(0);
        this.emptyEmitStreak = new MutableLong(0);
        this.spoutThrottlingMetrics = new SpoutThrottlingMetrics();
        this.pending = new RotatingMap<>(2, new RotatingMap.ExpiredCallback<Long, TupleInfo>() {
            @Override
            public void expire(Long key, TupleInfo tupleInfo) {
                Long timeDelta = null;
                if (tupleInfo.getTimestamp() != 0) {
                    timeDelta = Time.deltaMs(tupleInfo.getTimestamp());
                }
                ExecutorCommon.failSpoutMsg(executorData, idToTask.get(tupleInfo.getTaskId()), timeDelta, tupleInfo, "TIMEOUT");
            }
        });
        this.outputCollectors = new ArrayList<>();

        init();
    }

    @Override
    protected void init() {
        LOG.info("Opening spout {}:{}", componentId, idToTask.keySet());
        this.spoutThrottlingMetrics.registerAll(stormConf, idToTask.values().iterator().next().getUserContext());
        for (Map.Entry<Integer, Task> entry : idToTask.entrySet()) {
            Task taskData = entry.getValue();
            ISpout spoutObject = (ISpout) taskData.getTaskObject();
            SpoutOutputCollectorImpl spoutOutputCollector = new SpoutOutputCollectorImpl(spoutObject, executorData, taskData, entry.getKey(), emittedCount,
                    hasAckers, rand, isEventLoggers, isDebug, pending);
            SpoutOutputCollector outputCollector = new SpoutOutputCollector(spoutOutputCollector);
            this.outputCollectors.add(outputCollector);

            taskData.getBuiltInMetrics().registerAll(stormConf, taskData.getUserContext());
            Map<String, DisruptorQueue> map = ImmutableMap.of("sendqueue", transferQueue, "receive", receiveQueue);
            BuiltinMetricsUtil.registerQueueMetrics(map, stormConf, taskData.getUserContext());

            if (spoutObject instanceof ICredentialsListener) {
                ((ICredentialsListener) spoutObject).setCredentials(credentials);
            }
            spoutObject.open(stormConf, taskData.getUserContext(), outputCollector);
        }
        executorData.setOpenOrPrepareWasCalled(true);
        LOG.info("Opened spout {}:{}", componentId, idToTask.keySet());
        setupMetrics();
    }

    @Override
    public Object call() throws Exception {
        while (!executorData.getStormActive().get()) {
            Utils.sleep(100);
        }
        receiveQueue.consumeBatch(this);

        long currCount = emittedCount.get();
        boolean throttleOn = backPressureEnabled && this.throttleOn.get();
        boolean reachedMaxSpoutPending = (maxSpoutPending != 0) && (pending.size() >= maxSpoutPending);
        boolean isActive = executorData.getStormActive().get();
        if (isActive) {
            if (!lastActive.get()) {
                lastActive.set(true);
                LOG.info("Activating spout {}:{}", componentId, idToTask.keySet());
                for (ISpout spout : spouts) {
                    spout.activate();
                }
            }
            if (!transferQueue.isFull() && !throttleOn && !reachedMaxSpoutPending) {
                for (ISpout spout : spouts) {
                    spout.nextTuple();
                }
            }
        } else {
            if (lastActive.get()) {
                lastActive.set(false);
                LOG.info("Deactivating spout {}:{}", componentId, idToTask.keySet());
                for (ISpout spout : spouts) {
                    spout.deactivate();
                }
            }
            Time.sleep(100);
            spoutThrottlingMetrics.skippedInactive(executorData.getStats());
        }
        if (currCount == emittedCount.get() && isActive) {
            emptyEmitStreak.increment();
            spoutWaitStrategy.emptyEmit(emptyEmitStreak.get());
            if (throttleOn) {
                spoutThrottlingMetrics.skippedThrottle(executorData.getStats());
            } else if (reachedMaxSpoutPending) {
                spoutThrottlingMetrics.skippedMaxSpout(executorData.getStats());
            }
        } else {
            emptyEmitStreak.set(0);
        }
        return 0L;
    }

    @Override
    public void tupleActionFn(int taskId, TupleImpl tuple) throws Exception {
        String streamId = tuple.getSourceStreamId();
        if (streamId.equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            pending.rotate();
        } else if (streamId.equals(Constants.METRICS_TICK_STREAM_ID)) {
            metricsTick(idToTask.get(taskId), tuple);
        } else if (streamId.equals(Constants.CREDENTIALS_CHANGED_STREAM_ID)) {
            Object spoutObj = idToTask.get(taskId).getTaskObject();
            if (spoutObj instanceof ICredentialsListener) {
                ((ICredentialsListener) spoutObj).setCredentials((Map<String, String>) tuple.getValue(0));
            }
        } else if (streamId.equals(Acker.ACKER_RESET_TIMEOUT_STREAM_ID)) {
            Long id = (Long) tuple.getValue(0);
            TupleInfo pendingForId = pending.get(id);
            if (pendingForId != null) {
                pending.put(id, pendingForId);
            }
        } else {
            Long id = (Long) tuple.getValue(0);
            TupleInfo tupleInfo = (TupleInfo) pending.remove(id);
            if (tupleInfo.getMessageId() != null) {
                if (taskId != tupleInfo.getTaskId()) {
                    throw new RuntimeException("Fatal error, mismatched task ids: " + taskId + " " + tupleInfo.getTaskId());
                }
                long startTimeMs = tupleInfo.getTimestamp();
                Long timeDelta = null;
                if (startTimeMs != 0) {
                    timeDelta = Time.deltaMs(startTimeMs);
                }
                if (streamId.equals(Acker.ACKER_ACK_STREAM_ID)) {
                    ExecutorCommon.ackSpoutMsg(executorData, idToTask.get(taskId), timeDelta, tupleInfo);
                } else if (streamId.equals(Acker.ACKER_FAIL_STREAM_ID)) {
                    ExecutorCommon.failSpoutMsg(executorData, idToTask.get(taskId), timeDelta, tupleInfo, "FAIL-STREAM");
                }
            }
        }
    }
}
