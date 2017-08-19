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
package org.apache.storm.executor.bolt;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.metrics.BuiltinMetricsUtil;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.executor.Executor;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.policy.IWaitStrategy.WAIT_SITUATION;
import org.apache.storm.policy.WaitStrategyProgressive;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.RunningAvg;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BooleanSupplier;

public class BoltExecutor extends Executor {

    private static final Logger LOG = LoggerFactory.getLogger(BoltExecutor.class);

    private final BooleanSupplier executeSampler;
    private final boolean isSystemBoltExecutor;
    private final IWaitStrategy consumeWaitStrategy;
    private BoltOutputCollectorImpl outputCollector;

    public BoltExecutor(WorkerState workerData, List<Long> executorId, Map<String, String> credentials) {
        super(workerData, executorId, credentials);
        this.executeSampler = ConfigUtils.mkStatsSampler(topoConf);
        this.isSystemBoltExecutor =  (executorId == Constants.SYSTEM_EXECUTOR_ID );
        if (isSystemBoltExecutor)
            this.consumeWaitStrategy = new WaitStrategyProgressive();
        else
            this.consumeWaitStrategy = ReflectionUtils.newInstance((String) topoConf.get(Config.TOPOLOGY_BOLT_WAIT_STRATEGY));
        this.consumeWaitStrategy.prepare(topoConf, WAIT_SITUATION.BOLT_WAIT);
    }

    public void init(ArrayList<Task> idToTask, int idToTaskBase) {
        while (!stormActive.get()) {
            Utils.sleep(100);
        }

        if (!componentId.equals(StormCommon.SYSTEM_STREAM_ID)) { // System bolt doesn't call reportError()
            this.errorReportingMetrics.registerAll(topoConf, idToTask.get(taskIds.get(0) - idToTaskBase).getUserContext());
        }
        LOG.info("Preparing bolt {}:{}", componentId, getTaskIds());
        for (Task taskData : idToTask) {
            if (taskData == null) {
                //This happens if the min id is too small
                continue;
            }
            IBolt boltObject = (IBolt) taskData.getTaskObject();
            TopologyContext userContext = taskData.getUserContext();
            taskData.getBuiltInMetrics().registerAll(topoConf, userContext);
            if (boltObject instanceof ICredentialsListener) {
                ((ICredentialsListener) boltObject).setCredentials(credentials);
            }
            if (Constants.SYSTEM_COMPONENT_ID.equals(componentId)) {
                Map<String, JCQueue> map = ImmutableMap.of("receive", receiveQueue, "transfer", workerData.getTransferQueue());
                BuiltinMetricsUtil.registerQueueMetrics(map, topoConf, userContext);

                Map cachedNodePortToSocket = workerData.getCachedNodeToPortSocket().get();
                BuiltinMetricsUtil.registerIconnectionClientMetrics(cachedNodePortToSocket, topoConf, userContext);
                BuiltinMetricsUtil.registerIconnectionServerMetric(workerData.getReceiver(), topoConf, userContext);
            } else {
                Map<String, JCQueue> map = ImmutableMap.of("receive", receiveQueue);
                BuiltinMetricsUtil.registerQueueMetrics(map, topoConf, userContext);
            }

            this.outputCollector = new BoltOutputCollectorImpl(this, taskData, rand, hasEventLoggers, ackingEnabled, isDebug);
            boltObject.prepare(topoConf, userContext, new OutputCollector(outputCollector));
        }
        openOrPrepareWasCalled.set(true);
        LOG.info("Prepared bolt {}:{}", componentId, taskIds);
        setupMetrics();
    }

    @Override
    public Callable<Long> call() throws Exception {
        init(idToTask, idToTaskBase);

        return new Callable<Long>() {
            RunningAvg avgConsumeCount = new RunningAvg("BOLT Avg consume count", 10_000_000, true);
            @Override
            public Long call() throws Exception {
                int count = receiveQueue.consume(BoltExecutor.this);
                for(int idleCounter=0; count==0; ) {
                    idleCounter = consumeWaitStrategy.idle(idleCounter);
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                    count = receiveQueue.consume(BoltExecutor.this);
                }
                avgConsumeCount.push(count);
                return 0L;
            }
        };
    }

    @Override
    public void tupleActionFn(int taskId, TupleImpl tuple) throws Exception {
        String streamId = tuple.getSourceStreamId();
        if (Constants.SYSTEM_FLUSH_STREAM_ID.equals(streamId)) {
            outputCollector.flush();
        } else if (Constants.METRICS_TICK_STREAM_ID.equals(streamId)) {
            metricsTick(idToTask.get(taskId - idToTaskBase), tuple);
        } else if (Constants.CREDENTIALS_CHANGED_STREAM_ID.equals(streamId)) {
            Object taskObject = idToTask.get(taskId - idToTaskBase).getTaskObject();
            if (taskObject instanceof ICredentialsListener) {
                ((ICredentialsListener) taskObject).setCredentials((Map<String, String>) tuple.getValue(0));
            }
        } else {
            IBolt boltObject = (IBolt) idToTask.get(taskId - idToTaskBase).getTaskObject();
            boolean isSampled = sampler.getAsBoolean();
            boolean isExecuteSampler = executeSampler.getAsBoolean();
            Long now = (isSampled || isExecuteSampler) ? Time.currentTimeMillis() : null;
            if (isSampled) {
                tuple.setProcessSampleStartTime(now);
            }
            if (isExecuteSampler) {
                tuple.setExecuteSampleStartTime(now);
            }
            boltObject.execute(tuple);

            Long ms = tuple.getExecuteSampleStartTime();
            long delta = (ms != null) ? Time.deltaMs(ms) : -1;
            if (isDebug) {
                LOG.info("Execute done TUPLE {} TASK: {} DELTA: {}", tuple, taskId, delta);
            }
            TopologyContext topologyContext = idToTask.get(taskId - idToTaskBase).getUserContext();
            if (!topologyContext.getHooks().isEmpty()) // perf critical check to avoid unnecessary allocation
                new BoltExecuteInfo(tuple, taskId, delta).applyOn(topologyContext);
            if (delta >= 0) {
                ((BoltExecutorStats) stats).boltExecuteTuple(tuple.getSourceComponent(), tuple.getSourceStreamId(), delta);
            }
        }
    }

}
