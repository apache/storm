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
import org.apache.storm.utils.RunningAvg;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.JCQueue;
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
    SpoutOutputCollectorImpl spoutOutputCollector;
    private RunningAvg latencySampled;

    public SpoutExecutor(final WorkerState workerData, final List<Long> executorId, Map<String, String> credentials) {
        super(workerData, executorId, credentials);
        this.spoutWaitStrategy = ReflectionUtils.newInstance((String) topoConf.get(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY));
        this.spoutWaitStrategy.prepare(topoConf);

        this.lastActive = new AtomicBoolean(false);
        this.hasAckers = StormCommon.hasAckers(topoConf);
        this.emittedCount = new MutableLong(0);
        this.emptyEmitStreak = new MutableLong(0);
        this.spoutThrottlingMetrics = new SpoutThrottlingMetrics();
    }

    public void init(final ArrayList<Task> idToTask, int idToTaskBase) {
        latencySampled = new RunningAvg("[SAMPLED] Latency", 10_000_000);
        while (!stormActive.get()) {
            Utils.sleep(100);
        }

        LOG.info("Opening spout {}:{}", componentId, taskIds );
        this.idToTask = idToTask;
        this.maxSpoutPending = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), 0) * idToTask.size();
        this.spouts = new ArrayList<>();
        for (Task task : idToTask) {
            if(task!=null)
                this.spouts.add((ISpout) task.getTaskObject());
        }
        this.pending = new RotatingMap<>(2, new RotatingMap.ExpiredCallback<Long, TupleInfo>() {
            @Override
            public void expire(Long key, TupleInfo tupleInfo) {
                Long timeDelta = null;
                if (tupleInfo.getTimestamp() != 0) {
                    timeDelta = Time.deltaMs(tupleInfo.getTimestamp());
                }
                failSpoutMsg(SpoutExecutor.this, idToTask.get(tupleInfo.getTaskId() - idToTaskBase), timeDelta, tupleInfo, "TIMEOUT");
            }
        });

        this.spoutThrottlingMetrics.registerAll(topoConf, idToTask.get(taskIds.get(0) - idToTaskBase).getUserContext());
        this.errorReportingMetrics.registerAll(topoConf, idToTask.get(taskIds.get(0) - idToTaskBase).getUserContext());
        this.outputCollectors = new ArrayList<>();
        for (int i=0; i<idToTask.size(); ++i) {
            Task taskData = idToTask.get(i);
            if (taskData==null) {
                continue;
            }
            ISpout spoutObject = (ISpout) taskData.getTaskObject();
            spoutOutputCollector = new SpoutOutputCollectorImpl(
                    spoutObject, this, taskData, emittedCount,
                    hasAckers, rand, hasEventLoggers, isDebug, pending);
            SpoutOutputCollector outputCollector = new SpoutOutputCollector(spoutOutputCollector);
            this.outputCollectors.add(outputCollector);

            taskData.getBuiltInMetrics().registerAll(topoConf, taskData.getUserContext());
            Map<String, JCQueue> map = ImmutableMap.of("receive", receiveQueue);
            BuiltinMetricsUtil.registerQueueMetrics(map, topoConf, taskData.getUserContext());

            if (spoutObject instanceof ICredentialsListener) {
                ((ICredentialsListener) spoutObject).setCredentials(credentials);
            }
            spoutObject.open(topoConf, taskData.getUserContext(), outputCollector);
        }
        openOrPrepareWasCalled.set(true);
        LOG.info("Opened spout {}:{}", componentId, taskIds);
        setupMetrics();
    }

    @Override
    public Callable<Long> call() throws Exception {
        init(idToTask, idToTaskBase);
        return new Callable<Long>() {
            int i=0;
            final int recvqCheckSkipCount = getSpoutRecvqCheckSkipCount();
            @Override
            public Long call() throws Exception {
                if (i++ == recvqCheckSkipCount) {
                    receiveQueue.consume(SpoutExecutor.this);
                    i=0;
                }

                long currCount = emittedCount.get();
                boolean reachedMaxSpoutPending = (maxSpoutPending != 0) && (pending.size() >= maxSpoutPending);
                boolean isActive = stormActive.get();
                if (isActive) {
                    if (!lastActive.get()) {
                        lastActive.set(true);
                        LOG.info("Activating spout {}:{}", componentId, taskIds);
                        for (ISpout spout : spouts) {
                            spout.activate();
                        }
                    }
                    if ( !reachedMaxSpoutPending ) {
                        for (int j = 0; j < spouts.size(); j++) { // perf critical loop. dont use iterators.
                            spouts.get(j).nextTuple();
                        }
                    }
                } else {
                    if (lastActive.get()) {
                        lastActive.set(false);
                        LOG.info("Deactivating spout {}:{}", componentId, taskIds);
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
                    spoutThrottlingMetrics.skippedMaxSpoutMs(Time.currentTimeMillis() - start);
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
        if (Constants.SYSTEM_FLUSH_STREAM_ID.equals(streamId)) {
            spoutOutputCollector.flush();
        } else if (streamId.equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            pending.rotate();
        } else if (streamId.equals(Constants.METRICS_TICK_STREAM_ID)) {
            metricsTick(idToTask.get(taskId - idToTaskBase), tuple);
        } else if (streamId.equals(Constants.CREDENTIALS_CHANGED_STREAM_ID)) {
            Object spoutObj = idToTask.get(taskId - idToTaskBase).getTaskObject();
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
            TupleInfo tupleInfo = pending.remove(id);
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
                    ackSpoutMsg(this, idToTask.get(taskId - idToTaskBase), timeDelta, tupleInfo);
                } else if (streamId.equals(Acker.ACKER_FAIL_STREAM_ID)) {
                    failSpoutMsg(this, idToTask.get(taskId - idToTaskBase), timeDelta, tupleInfo, "FAIL-STREAM");
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
            if (!taskData.getUserContext().getHooks().isEmpty()) // avoid allocating SpoutAckInfo obj if not necessary
                new SpoutAckInfo(tupleInfo.getMessageId(), taskId, timeDelta).applyOn(taskData.getUserContext());
            if (timeDelta != null && hasAckers) {
                latencySampled.push(timeDelta);
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


    public int getSpoutRecvqCheckSkipCount() {
        if(ackingEnabled)
            return 0; // always check recQ if ACKing enabled
        return ObjectReader.getInt(conf.get(Config.TOPOLOGY_SPOUT_RECVQ_SKIPS), 0);
    }
}
