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
import java.util.concurrent.Callable;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.metrics.BuiltinMetricsUtil;
import org.apache.storm.daemon.metrics.SpoutThrottlingMetrics;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.executor.Executor;
import org.apache.storm.executor.TupleInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.ISpoutWaitStrategy;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.stats.SpoutExecutorStats;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.MutableLong;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SpoutExecutor extends Executor {

    private static final Logger LOG = LoggerFactory.getLogger(SpoutExecutor.class);

    private final ISpoutWaitStrategy spoutWaitStrategy;
    private Integer maxSpoutPending;
    private final AtomicBoolean lastActive;
    private List<ISpout> spouts;
    private List<SpoutOutputCollector> outputCollectors;
    private final MutableLong emittedCount;
    private final MutableLong emptyEmitStreak;
    private final SpoutThrottlingMetrics spoutThrottlingMetrics;
    private final boolean hasAckers;
    private RotatingMap<Long, TupleInfo> pending;
    private final boolean backPressureEnabled;

    public SpoutExecutor(final WorkerState workerData, final List<Long> executorId, Map<String, String> credentials) {
        super(workerData, executorId, credentials);
        this.spoutWaitStrategy = ReflectionUtils.newInstance((String) topoConf.get(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY));
        this.spoutWaitStrategy.prepare(topoConf);

        this.backPressureEnabled = ObjectReader.getBoolean(topoConf.get(Config.TOPOLOGY_BACKPRESSURE_ENABLE), false);

        this.lastActive = new AtomicBoolean(false);
        this.hasAckers = StormCommon.hasAckers(topoConf);
        this.emittedCount = new MutableLong(0);
        this.emptyEmitStreak = new MutableLong(0);
        this.spoutThrottlingMetrics = new SpoutThrottlingMetrics();
    }

    public void init(final Map<Integer, Task> idToTask) {
        while (!stormActive.get()) {
            Utils.sleep(100);
        }

        LOG.info("Opening spout {}:{}", componentId, idToTask.keySet());
        this.idToTask = idToTask;
        this.maxSpoutPending = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), 0) * idToTask.size();
        this.spouts = new ArrayList<>();
        for (Task task : idToTask.values()) {
            this.spouts.add((ISpout) task.getTaskObject());
        }
        this.pending = new RotatingMap<>(2, new RotatingMap.ExpiredCallback<Long, TupleInfo>() {
            @Override
            public void expire(Long key, TupleInfo tupleInfo) {
                Long timeDelta = null;
                if (tupleInfo.getTimestamp() != 0) {
                    timeDelta = Time.deltaMs(tupleInfo.getTimestamp());
                }
                failSpoutMsg(SpoutExecutor.this, idToTask.get(tupleInfo.getTaskId()), timeDelta, tupleInfo, "TIMEOUT");
            }
        });

        this.spoutThrottlingMetrics.registerAll(topoConf, idToTask.values().iterator().next().getUserContext());
        this.errorReportingMetrics.registerAll(topoConf, idToTask.values().iterator().next().getUserContext());
        this.outputCollectors = new ArrayList<>();
        for (Map.Entry<Integer, Task> entry : idToTask.entrySet()) {
            Task taskData = entry.getValue();
            ISpout spoutObject = (ISpout) taskData.getTaskObject();
            SpoutOutputCollectorImpl spoutOutputCollector = new SpoutOutputCollectorImpl(
                    spoutObject, this, taskData, entry.getKey(), emittedCount,
                    hasAckers, rand, hasEventLoggers, isDebug, pending);
            SpoutOutputCollector outputCollector = new SpoutOutputCollector(spoutOutputCollector);
            this.outputCollectors.add(outputCollector);

            taskData.getBuiltInMetrics().registerAll(topoConf, taskData.getUserContext());
            Map<String, DisruptorQueue> map = ImmutableMap.of("sendqueue", sendQueue, "receive", receiveQueue);
            BuiltinMetricsUtil.registerQueueMetrics(map, topoConf, taskData.getUserContext());

            if (spoutObject instanceof ICredentialsListener) {
                ((ICredentialsListener) spoutObject).setCredentials(credentials);
            }
            spoutObject.open(topoConf, taskData.getUserContext(), outputCollector);
        }
        openOrPrepareWasCalled.set(true);
        LOG.info("Opened spout {}:{}", componentId, idToTask.keySet());
        setupMetrics();
    }

    @Override
    public Callable<Object> call() throws Exception {
        init(idToTask);

        return new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                receiveQueue.consumeBatch(SpoutExecutor.this);

                final long currCount = emittedCount.get();
                final boolean throttleOn = backPressureEnabled && SpoutExecutor.this.throttleOn.get();
                final boolean reachedMaxSpoutPending = (maxSpoutPending != 0) && (pending.size() >= maxSpoutPending);
                final boolean isActive = stormActive.get();
                if (isActive) {
                    if (!lastActive.get()) {
                        lastActive.set(true);
                        LOG.info("Activating spout {}:{}", componentId, idToTask.keySet());
                        for (ISpout spout : spouts) {
                            spout.activate();
                        }
                    }
                    if (!sendQueue.isFull() && !throttleOn && !reachedMaxSpoutPending) {
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
                    long start = Time.currentTimeMillis();
                    Time.sleep(100);
                    spoutThrottlingMetrics.skippedInactiveMs(Time.currentTimeMillis() - start);
                }
                if (currCount == emittedCount.get() && isActive) {
                    emptyEmitStreak.increment();
                    long start = Time.currentTimeMillis();
                    spoutWaitStrategy.emptyEmit(emptyEmitStreak.get());
                    if (throttleOn) {
                        spoutThrottlingMetrics.skippedThrottleMs(Time.currentTimeMillis() - start);
                    } else if (reachedMaxSpoutPending) {
                        spoutThrottlingMetrics.skippedMaxSpoutMs(Time.currentTimeMillis() - start);
                    }
                } else {
                    emptyEmitStreak.set(0);
                }
                return 0L;
            }
        };
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
            Long timeDeltaMs = (Long) tuple.getValue(1);
            TupleInfo tupleInfo = (TupleInfo) pending.remove(id);
            if (tupleInfo != null && tupleInfo.getMessageId() != null) {
                if (taskId != tupleInfo.getTaskId()) {
                    throw new RuntimeException("Fatal error, mismatched task ids: " + taskId + " " + tupleInfo.getTaskId());
                }
                long startTimeMs = tupleInfo.getTimestamp();
                Long timeDelta = null;
                if (startTimeMs != 0) {
                    timeDelta = timeDeltaMs;
                }
                if (streamId.equals(Acker.ACKER_ACK_STREAM_ID)) {
                    ackSpoutMsg(this, idToTask.get(taskId), timeDelta, tupleInfo);
                } else if (streamId.equals(Acker.ACKER_FAIL_STREAM_ID)) {
                    failSpoutMsg(this, idToTask.get(taskId), timeDelta, tupleInfo, "FAIL-STREAM");
                }
            }
        }
    }

    public void ackSpoutMsg(Executor executor, Task taskData, Long timeDelta, TupleInfo tupleInfo) {
        try {
            ISpout spout = (ISpout) taskData.getTaskObject();
            int taskId = taskData.getTaskId();
            if (executor.getIsDebug()) {
                LOG.info("SPOUT Acking message {} {}", tupleInfo.getId(), tupleInfo.getMessageId());
            }
            spout.ack(tupleInfo.getMessageId());
            new SpoutAckInfo(tupleInfo.getMessageId(), taskId, timeDelta).applyOn(taskData.getUserContext());
            if (timeDelta != null) {
                ((SpoutExecutorStats) executor.getStats()).spoutAckedTuple(tupleInfo.getStream(), timeDelta);
            }
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public void failSpoutMsg(Executor executor, Task taskData, Long timeDelta, TupleInfo tupleInfo, String reason) {
        try {
            ISpout spout = (ISpout) taskData.getTaskObject();
            int taskId = taskData.getTaskId();
            if (executor.getIsDebug()) {
                LOG.info("SPOUT Failing {} : {} REASON: {}", tupleInfo.getId(), tupleInfo, reason);
            }
            spout.fail(tupleInfo.getMessageId());
            new SpoutFailInfo(tupleInfo.getMessageId(), taskId, timeDelta).applyOn(taskData.getUserContext());
            if (timeDelta != null) {
                ((SpoutExecutorStats) executor.getStats()).spoutFailedTuple(tupleInfo.getStream(), timeDelta);
            }
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }
}
