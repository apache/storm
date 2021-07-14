/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.executor.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BooleanSupplier;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.metrics.BuiltinMetricsUtil;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.executor.Executor;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.metric.api.IMetricsRegistrant;
import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.policy.IWaitStrategy.WaitSituation;
import org.apache.storm.policy.WaitStrategyPark;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.stats.ClientStatsUtil;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.JCQueue.ExitCondition;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BoltExecutor extends Executor {

    private static final Logger LOG = LoggerFactory.getLogger(BoltExecutor.class);

    private final BooleanSupplier executeSampler;
    private final boolean isSystemBoltExecutor;
    private final IWaitStrategy consumeWaitStrategy;       // employed when no incoming data
    private final IWaitStrategy backPressureWaitStrategy;  // employed when outbound path is congested
    private final BoltExecutorStats stats;
    private BoltOutputCollectorImpl outputCollector;

    public BoltExecutor(WorkerState workerData, List<Long> executorId, Map<String, String> credentials) {
        super(workerData, executorId, credentials, ClientStatsUtil.BOLT);
        this.executeSampler = ConfigUtils.mkStatsSampler(topoConf);
        this.isSystemBoltExecutor = (executorId == Constants.SYSTEM_EXECUTOR_ID);
        if (isSystemBoltExecutor) {
            this.consumeWaitStrategy = makeSystemBoltWaitStrategy();
        } else {
            this.consumeWaitStrategy = ReflectionUtils.newInstance((String) topoConf.get(Config.TOPOLOGY_BOLT_WAIT_STRATEGY));
            this.consumeWaitStrategy.prepare(topoConf, WaitSituation.BOLT_WAIT);
        }
        this.backPressureWaitStrategy = ReflectionUtils.newInstance((String) topoConf.get(Config.TOPOLOGY_BACKPRESSURE_WAIT_STRATEGY));
        this.backPressureWaitStrategy.prepare(topoConf, WaitSituation.BACK_PRESSURE_WAIT);
        this.stats = new BoltExecutorStats(ConfigUtils.samplingRate(this.getTopoConf()),
                                           ObjectReader.getInt(this.getTopoConf().get(Config.NUM_STAT_BUCKETS)));
    }

    private static IWaitStrategy makeSystemBoltWaitStrategy() {
        WaitStrategyPark ws = new WaitStrategyPark();
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_BOLT_WAIT_PARK_MICROSEC, 5000);
        ws.prepare(conf, WaitSituation.BOLT_WAIT);
        return ws;
    }

    @Override
    public BoltExecutorStats getStats() {
        return stats;
    }

    public void init(ArrayList<Task> idToTask, int idToTaskBase) throws InterruptedException {
        executorTransfer.initLocalRecvQueues();
        workerReady.await();
        while (!stormActive.get()) {
            //Topology may be deployed in deactivated mode, wait for activation
            Utils.sleepNoSimulation(100);
        }

        LOG.info("Preparing bolt {}:{}", componentId, getTaskIds());
        for (Task taskData : idToTask) {
            if (taskData == null) {
                //This happens if the min id is too small
                continue;
            }
            IBolt boltObject = (IBolt) taskData.getTaskObject();
            TopologyContext userContext = taskData.getUserContext();
            if (boltObject instanceof ICredentialsListener) {
                ((ICredentialsListener) boltObject).setCredentials(credentials);
            }
            if (Constants.SYSTEM_COMPONENT_ID.equals(componentId)) {
                BuiltinMetricsUtil.registerIconnectionServerMetric(workerData.getReceiver(), topoConf, userContext);

                // add any autocredential expiry metrics from the worker
                if (workerData.getAutoCredentials() != null) {
                    for (IAutoCredentials autoCredential : workerData.getAutoCredentials()) {
                        if (autoCredential instanceof IMetricsRegistrant) {
                            IMetricsRegistrant registrant = (IMetricsRegistrant) autoCredential;
                            registrant.registerMetrics(userContext, topoConf);
                        }
                    }
                }
            }

            this.outputCollector = new BoltOutputCollectorImpl(this, taskData, rand, hasEventLoggers, ackingEnabled, isDebug);
            boltObject.prepare(topoConf, userContext, new OutputCollector(outputCollector));
        }
        openOrPrepareWasCalled.set(true);
        LOG.info("Prepared bolt {}:{}", componentId, taskIds);
        setupTicks(false);
        setupMetrics();
    }

    @Override
    public Callable<Long> call() throws Exception {
        init(idToTask, idToTaskBase);

        return new Callable<Long>() {
            int bpIdleCount = 0;
            int consumeIdleCounter = 0;
            private final ExitCondition tillNoPendingEmits = () -> pendingEmits.isEmpty();

            @Override
            public Long call() throws Exception {
                updateExecCredsIfRequired();
                boolean pendingEmitsIsEmpty = tryFlushPendingEmits();
                if (pendingEmitsIsEmpty) {
                    if (bpIdleCount != 0) {
                        LOG.debug("Ending Back Pressure Wait stretch : {}", bpIdleCount);
                    }
                    bpIdleCount = 0;
                    int consumeCount = receiveQueue.consume(BoltExecutor.this, tillNoPendingEmits);
                    if (consumeCount == 0) {
                        if (consumeIdleCounter == 0) {
                            LOG.debug("Invoking consume wait strategy");
                        }
                        consumeIdleCounter = consumeWaitStrategy.idle(consumeIdleCounter);
                        if (Thread.interrupted()) {
                            throw new InterruptedException();
                        }
                    } else {
                        if (consumeIdleCounter != 0) {
                            LOG.debug("Ending consume wait stretch : {}", consumeIdleCounter);
                        }
                        consumeIdleCounter = 0;
                    }
                } else {
                    if (bpIdleCount == 0) { // check avoids multiple log msgs when spinning in a idle loop
                        LOG.debug("Experiencing Back Pressure. Entering BackPressure Wait. PendingEmits = {}", pendingEmits.size());
                    }
                    bpIdleCount = backPressureWaitStrategy.idle(bpIdleCount);
                }

                return 0L;
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

    @Override
    public void tupleActionFn(int taskId, TupleImpl tuple) throws Exception {
        String streamId = tuple.getSourceStreamId();
        if (Constants.SYSTEM_FLUSH_STREAM_ID.equals(streamId)) {
            outputCollector.flush();
        } else if (Constants.METRICS_TICK_STREAM_ID.equals(streamId)) {
            metricsTick(idToTask.get(taskId - idToTaskBase), tuple);
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
            if (!topologyContext.getHooks().isEmpty()) {
                // perf critical check to avoid unnecessary allocation
                new BoltExecuteInfo(tuple, taskId, delta).applyOn(topologyContext);
            }
            if (delta >= 0) {
                Task firstTask = idToTask.get(taskIds.get(0) - idToTaskBase);
                stats.boltExecuteTuple(tuple.getSourceComponent(), tuple.getSourceStreamId(), delta,
                        workerData.getUptime().upTime(), firstTask);
                Task currentTask = idToTask.get(taskId - idToTaskBase);
                currentTask.getTaskMetrics().boltExecuteTuple(tuple.getSourceComponent(), tuple.getSourceStreamId(), delta);
            }
        }
    }
}
