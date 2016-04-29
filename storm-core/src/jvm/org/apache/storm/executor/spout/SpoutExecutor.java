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
    private final MutableLong emittedCount; // 共用
    private final MutableLong emptyEmitStreak; // 共用
    private final SpoutThrottlingMetrics spoutThrottlingMetrics; // 共用
    private final boolean isAcker; // 共用
    private final RotatingMap<Long, TupleInfo> pending; // 共用

    public SpoutExecutor(final ExecutorData executorData, final Map<Integer, Task> taskDatas, Map<String, String> credentials) {
        super(executorData, taskDatas, credentials);
        this.spoutWaitStrategy =  Utils.newInstance((String) stormConf.get(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY));
        this.spoutWaitStrategy.prepare(stormConf);
        this.maxSpoutPending = Utils.getInt(stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING)) * taskDatas.size();
        this.lastActive = new AtomicBoolean(false);
        this.spouts = new ArrayList<>();
        for (Task task : taskDatas.values()) {
            this.spouts.add((ISpout) task.getTaskObject());
        }
        this.isAcker = StormCommon.hasAckers(stormConf);
        this.emittedCount = new MutableLong(0);
        this.emptyEmitStreak = new MutableLong(0);
        this.spoutThrottlingMetrics = new SpoutThrottlingMetrics();
        this.pending = new RotatingMap(new RotatingMap.ExpiredCallback() {
            @Override
            public void expire(Object key, Object val) {
                TupleInfo tupleInfo = (TupleInfo) val;
                ExecutorCommon.failSpoutMsg(executorData, taskDatas.get(tupleInfo.getTaskId()), tupleInfo, "TIMEOUT");
            }
        });
        this.outputCollectors = new ArrayList<>();
        init();
    }

    @Override
    protected void init() {
        for (Map.Entry<Integer, Task> entry : taskDatas.entrySet()) {
            Task taskData = entry.getValue();
            ISpout spoutObject = (ISpout) taskData.getTaskObject();
            SpoutOutputCollectorImpl spoutOutputCollector = new SpoutOutputCollectorImpl(spoutObject, executorData, taskData, entry.getKey(), emittedCount,
                    isAcker, rand, isEventLoggers, isDebug, pending);
            SpoutOutputCollector OutputCollector = new SpoutOutputCollector(spoutOutputCollector);
            this.outputCollectors.add(OutputCollector);
            taskData.getBuiltInMetrics().registerAll(stormConf, taskData.getUserContext());
            Map<String, DisruptorQueue> map = ImmutableMap.of("sendqueue", transferQueue, "receive", receiveQueue);
            BuiltinMetricsUtil.registerQueueMetrics(map, stormConf, taskData.getUserContext());
            if (spoutObject instanceof ICredentialsListener) {
                ((ICredentialsListener) spoutObject).setCredentials(credentials);
            }
            spoutObject.open(stormConf, taskData.getUserContext(), OutputCollector);
        }
        executorData.setOpenOrprepareWasCalled(true);
        LOG.info("Opened spout {}:{}", componentId, taskDatas.keySet());
        setupMetrics();
    }

    @Override
    public Object call() throws Exception {
        receiveQueue.consumeBatch(this);

        long currCount = emittedCount.get();
        boolean backPressureEnable = Utils.getBoolean(stormConf.get(Config.TOPOLOGY_BACKPRESSURE_ENABLE), false);
        boolean throttleOn = (backPressureEnable && ((AtomicBoolean) (executorData.getWorkerData().get("throttle-on"))).get());
        boolean reachedMaxSpoutPending = ((maxSpoutPending != null) && (pending.size() > maxSpoutPending));
        if (executorData.getStormActiveAtom().get()) {
            if (!lastActive.get()) {
                lastActive.set(true);
                LOG.info("Activating spout {}:{}", componentId, taskDatas.keySet());
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
                LOG.info("Deactivating spout {}:{}", componentId, taskDatas.keySet());
                for (ISpout spout : spouts) {
                    spout.deactivate();
                }
            }
            Time.sleep(100);
            spoutThrottlingMetrics.skippedInactive(executorData.getStats());

        }
        if (currCount == emittedCount.get() && executorData.getStormActiveAtom().get()) {
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
        return (long) 0;
    }

    @Override
    public void tupleActionFn(int taskId, TupleImpl tuple) throws Exception {
        String streamId = tuple.getSourceStreamId();
        if (streamId.equals(Constants.SYSTEM_TICK_STREAM_ID))
            pending.rotate();
        else if (streamId.equals(Constants.METRICS_TICK_STREAM_ID)) {
            metricsTick(taskDatas.get(taskId), tuple);
        } else if (streamId.equals(Constants.CREDENTIALS_CHANGED_STREAM_ID)) {
            Object object = taskDatas.get(taskId).getTaskObject();
            if (object instanceof ICredentialsListener) {
                ((ICredentialsListener) object).setCredentials((Map<String, String>) tuple.getValue(0));
            }
        } else if (streamId.equals(Acker.ACKER_RESET_TIMEOUT_STREAM_ID)) {
            Long id = (Long) tuple.getValue(0);
            TupleInfo pendingForId = pending.get(id);
            if (pendingForId != null)
                pending.put(id, pendingForId);
        } else {
            Long id = (Long) tuple.getValue(0);
            TupleInfo tupleInfo = (TupleInfo) pending.remove(id);
            if (tupleInfo.getMessageId() != null) {
                if (taskId != tupleInfo.getTaskId()) {
                    throw new RuntimeException("Fatal error, mismatched task ids: " + taskId + " " + tupleInfo.getTaskId());
                }
                long startTimeMs = tupleInfo.getTimestamp();
                long timeDelta = 0;
                if (startTimeMs != 0)
                    timeDelta = Time.deltaMs(tupleInfo.getTimestamp());
                tupleInfo.setTimestamp(timeDelta);
                if (tupleInfo.getStream().equals(Acker.ACKER_ACK_STREAM_ID)) {
                    ExecutorCommon.ackSpoutMsg(executorData, taskDatas.get(taskId), tupleInfo);
                } else if (tupleInfo.getStream().equals(Acker.ACKER_FAIL_STREAM_ID)) {
                    ExecutorCommon.failSpoutMsg(executorData, taskDatas.get(taskId), tupleInfo, "FAIL-STREAM");
                }

            }
        }
    }
}
