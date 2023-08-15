/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.executor.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.executor.Executor;
import org.apache.storm.executor.TupleInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.metrics2.RateCounter;
import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.policy.IWaitStrategy.WaitSituation;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.stats.ClientStatsUtil;
import org.apache.storm.stats.SpoutExecutorStats;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.MutableLong;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpoutExecutor extends Executor {

    private static final Logger LOG = LoggerFactory.getLogger(SpoutExecutor.class);

    private final IWaitStrategy spoutWaitStrategy;
    private final IWaitStrategy backPressureWaitStrategy;
    private final AtomicBoolean lastActive;
    private final MutableLong emittedCount;
    private final MutableLong emptyEmitStreak;
    private final boolean hasAckers;
    private final SpoutExecutorStats stats;
    SpoutOutputCollectorImpl spoutOutputCollector;
    private Integer maxSpoutPending;
    private List<ISpout> spouts;
    private List<SpoutOutputCollector> outputCollectors;
    private RotatingMap<Long, TupleInfo> pending;
    private long threadId = 0;
    private final RateCounter skippedMaxSpoutMs;
    private final RateCounter skippedInactiveMs;
    private final RateCounter skippedBackpressureMs;

    public SpoutExecutor(final WorkerState workerData, final List<Long> executorId, Map<String, String> credentials) {
        super(workerData, executorId, credentials, ClientStatsUtil.SPOUT);
        this.spoutWaitStrategy = ReflectionUtils.newInstance((String) topoConf.get(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY));
        this.spoutWaitStrategy.prepare(topoConf, WaitSituation.SPOUT_WAIT);
        this.backPressureWaitStrategy = ReflectionUtils.newInstance((String) topoConf.get(Config.TOPOLOGY_BACKPRESSURE_WAIT_STRATEGY));
        this.backPressureWaitStrategy.prepare(topoConf, WaitSituation.BACK_PRESSURE_WAIT);

        this.lastActive = new AtomicBoolean(false);
        this.hasAckers = StormCommon.hasAckers(topoConf);
        this.emittedCount = new MutableLong(0);
        this.emptyEmitStreak = new MutableLong(0);
        this.stats = new SpoutExecutorStats(
            ConfigUtils.samplingRate(this.getTopoConf()), ObjectReader.getInt(this.getTopoConf().get(Config.NUM_STAT_BUCKETS)));
        this.skippedMaxSpoutMs = workerData.getMetricRegistry().rateCounter("__skipped-max-spout-ms", componentId,
                taskIds.get(0));
        this.skippedInactiveMs = workerData.getMetricRegistry().rateCounter("__skipped-inactive-ms", componentId,
                taskIds.get(0));
        this.skippedBackpressureMs = workerData.getMetricRegistry().rateCounter("__skipped-backpressure-ms", componentId,
                taskIds.get(0));
    }

    @Override
    public SpoutExecutorStats getStats() {
        return stats;
    }

    public void init(final ArrayList<Task> idToTask, int idToTaskBase) throws InterruptedException {
        this.threadId = Thread.currentThread().getId();
        executorTransfer.initLocalRecvQueues();
        workerReady.await();
        while (!stormActive.get()) {
            //Topology may be deployed in deactivated mode, wait for activation
            Utils.sleepNoSimulation(100);
        }

        LOG.info("Opening spout {}:{}", componentId, taskIds);
        this.idToTask = idToTask;
        this.maxSpoutPending = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), 0) * idToTask.size();
        this.spouts = new ArrayList<>();
        for (Task task : idToTask) {
            if (task != null) {
                this.spouts.add((ISpout) task.getTaskObject());
            }
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

        this.outputCollectors = new ArrayList<>();
        for (int i = 0; i < idToTask.size(); ++i) {
            Task taskData = idToTask.get(i);
            if (taskData == null) {
                continue;
            }
            ISpout spoutObject = (ISpout) taskData.getTaskObject();
            spoutOutputCollector = new SpoutOutputCollectorImpl(
                spoutObject, this, taskData, emittedCount,
                hasAckers, rand, hasEventLoggers, isDebug, pending);
            SpoutOutputCollector outputCollector = new SpoutOutputCollector(spoutOutputCollector);
            this.outputCollectors.add(outputCollector);

            if (spoutObject instanceof ICredentialsListener) {
                ((ICredentialsListener) spoutObject).setCredentials(credentials);
            }
            spoutObject.open(topoConf, taskData.getUserContext(), outputCollector);
        }
        openOrPrepareWasCalled.set(true);
        LOG.info("Opened spout {}:{}", componentId, taskIds);
        setupTicks(true);
        setupMetrics();
    }

    @Override
    public Callable<Long> call() throws Exception {
        init(idToTask, idToTaskBase);
        return new Callable<Long>() {
            final int recvqCheckSkipCountMax = getSpoutRecvqCheckSkipCount();
            int recvqCheckSkips = 0;
            int swIdleCount = 0; // counter for spout wait strategy
            int bpIdleCount = 0; // counter for back pressure wait strategy
            int rmspCount = 0;

            @Override
            public Long call() throws Exception {
                updateExecCredsIfRequired();
                int receiveCount = 0;
                if (recvqCheckSkips++ == recvqCheckSkipCountMax) {
                    receiveCount = receiveQueue.consume(SpoutExecutor.this);
                    recvqCheckSkips = 0;
                }
                long currCount = emittedCount.get();
                boolean reachedMaxSpoutPending = (maxSpoutPending != 0) && (pending.size() >= maxSpoutPending);
                boolean isActive = stormActive.get();

                if (!isActive) {
                    inactiveExecute();
                    return 0L;
                }

                if (!lastActive.get()) {
                    lastActive.set(true);
                    activateSpouts();
                }
                boolean pendingEmitsIsEmpty = tryFlushPendingEmits();
                boolean noEmits = true;
                long emptyStretch = 0;

                if (!reachedMaxSpoutPending && pendingEmitsIsEmpty) {
                    for (int j = 0; j < spouts.size(); j++) { // in critical path. don't use iterators.
                        spouts.get(j).nextTuple();
                    }
                    noEmits = (currCount == emittedCount.get());
                    if (noEmits) {
                        emptyEmitStreak.increment();
                    } else {
                        emptyStretch = emptyEmitStreak.get();
                        emptyEmitStreak.set(0);
                    }
                }
                if (reachedMaxSpoutPending) {
                    if (rmspCount == 0) {
                        LOG.debug("Reached max spout pending");
                    }
                    rmspCount++;
                } else {
                    if (rmspCount > 0) {
                        LOG.debug("Ended max spout pending stretch of {} iterations", rmspCount);
                    }
                    rmspCount = 0;
                }

                if (receiveCount > 1) {
                    // continue without idling
                    return 0L;
                }
                if (!pendingEmits.isEmpty()) { // then facing backpressure
                    backPressureWaitStrategy();
                    return 0L;
                }
                bpIdleCount = 0;
                if (noEmits) {
                    spoutWaitStrategy(reachedMaxSpoutPending, emptyStretch);
                    return 0L;
                }
                swIdleCount = 0;
                return 0L;
            }

            private void backPressureWaitStrategy() throws InterruptedException {
                long start = Time.currentTimeMillis();
                if (bpIdleCount == 0) { // check avoids multiple log msgs when in a idle loop
                    LOG.debug("Experiencing Back Pressure from downstream components. Entering BackPressure Wait.");
                }
                bpIdleCount = backPressureWaitStrategy.idle(bpIdleCount);
                skippedBackpressureMs.inc(Time.currentTimeMillis() - start);
            }

            private void spoutWaitStrategy(boolean reachedMaxSpoutPending, long emptyStretch) throws InterruptedException {
                emptyEmitStreak.increment();
                long start = Time.currentTimeMillis();
                swIdleCount = spoutWaitStrategy.idle(swIdleCount);
                if (reachedMaxSpoutPending) {
                    skippedMaxSpoutMs.inc(Time.currentTimeMillis() - start);
                } else {
                    if (emptyStretch > 0) {
                        LOG.debug("Ending Spout Wait Stretch of {}", emptyStretch);
                    }
                }
            }

            // returns true if pendingEmits is empty
            private boolean tryFlushPendingEmits() {
                for (AddressedTuple t = pendingEmits.peek(); t != null; t = pendingEmits.peek()) {
                    if (executorTransfer.tryTransfer(t, null)) {
                        pendingEmits.poll();
                    } else { // to avoid reordering of emits, stop at first failure
                        return false;
                    }
                }
                return true;
            }
        };
    }

    private void activateSpouts() {
        LOG.info("Activating spout {}:{}", componentId, taskIds);
        for (ISpout spout : spouts) {
            spout.activate();
        }
    }

    private void deactivateSpouts() {
        LOG.info("Deactivating spout {}:{}", componentId, taskIds);
        for (ISpout spout : spouts) {
            spout.deactivate();
        }
    }

    private void inactiveExecute() throws InterruptedException {
        if (lastActive.get()) {
            lastActive.set(false);
            deactivateSpouts();
        }
        long start = Time.currentTimeMillis();
        Time.sleep(100);
        skippedInactiveMs.inc(Time.currentTimeMillis() - start);
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
                Long timeDelta = null;
                if (hasAckers) {
                    long startTimeMs = tupleInfo.getTimestamp();
                    if (startTimeMs != 0) {
                        timeDelta = timeDeltaMs;
                    }
                }
                if (streamId.equals(Acker.ACKER_ACK_STREAM_ID)) {
                    ackSpoutMsg(this, idToTask.get(taskId - idToTaskBase), timeDelta, tupleInfo);
                } else if (streamId.equals(Acker.ACKER_FAIL_STREAM_ID)) {
                    failSpoutMsg(this, idToTask.get(taskId - idToTaskBase), timeDelta, tupleInfo, "FAIL-STREAM");
                }
            }
        }
    }

    public void ackSpoutMsg(SpoutExecutor executor, Task taskData, Long timeDelta, TupleInfo tupleInfo) {
        try {
            ISpout spout = (ISpout) taskData.getTaskObject();
            int taskId = taskData.getTaskId();
            if (executor.getIsDebug()) {
                LOG.info("SPOUT Acking message {} {}", tupleInfo.getRootId(), tupleInfo.getMessageId());
            }
            spout.ack(tupleInfo.getMessageId());
            if (!taskData.getUserContext().getHooks().isEmpty()) { // avoid allocating SpoutAckInfo obj if not necessary
                new SpoutAckInfo(tupleInfo.getMessageId(), taskId, timeDelta).applyOn(taskData.getUserContext());
            }
            if (hasAckers && timeDelta != null) {
                executor.getStats().spoutAckedTuple(tupleInfo.getStream(), timeDelta);
                taskData.getTaskMetrics().spoutAckedTuple(tupleInfo.getStream(), timeDelta);
            }
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public void failSpoutMsg(SpoutExecutor executor, Task taskData, Long timeDelta, TupleInfo tupleInfo, String reason) {
        try {
            ISpout spout = (ISpout) taskData.getTaskObject();
            int taskId = taskData.getTaskId();
            if (executor.getIsDebug()) {
                LOG.info("SPOUT Failing {} : {} REASON: {}", tupleInfo.getRootId(), tupleInfo, reason);
            }
            spout.fail(tupleInfo.getMessageId());
            new SpoutFailInfo(tupleInfo.getMessageId(), taskId, timeDelta).applyOn(taskData.getUserContext());
            if (timeDelta != null) {
                executor.getStats().spoutFailedTuple(tupleInfo.getStream());
                taskData.getTaskMetrics().spoutFailedTuple(tupleInfo.getStream());
            }
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }


    public int getSpoutRecvqCheckSkipCount() {
        if (ackingEnabled) {
            return 0; // always check recQ if ACKing enabled
        }
        return ObjectReader.getInt(conf.get(Config.TOPOLOGY_SPOUT_RECVQ_SKIPS), 0);
    }

    public long getThreadId() {
        return threadId;
    }   
    
}
