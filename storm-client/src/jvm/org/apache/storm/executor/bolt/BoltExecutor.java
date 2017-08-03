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
import java.util.List;
import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.metrics.BuiltinMetricsUtil;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.executor.Executor;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;

public class BoltExecutor extends Executor {

    private static final Logger LOG = LoggerFactory.getLogger(BoltExecutor.class);

    private final Callable<Boolean> executeSampler;

    public BoltExecutor(WorkerState workerData, List<Long> executorId, Map<String, String> credentials) {
        super(workerData, executorId, credentials);
        this.executeSampler = ConfigUtils.mkStatsSampler(topoConf);
    }

    public void init(Map<Integer, Task> idToTask) {
        while (!stormActive.get()) {
            Utils.sleep(100);
        }

        this.errorReportingMetrics.registerAll(topoConf, idToTask.values().iterator().next().getUserContext());

        LOG.info("Preparing bolt {}:{}", componentId, idToTask.keySet());
        for (Map.Entry<Integer, Task> entry : idToTask.entrySet()) {
            Task taskData = entry.getValue();
            IBolt boltObject = (IBolt) taskData.getTaskObject();
            TopologyContext userContext = taskData.getUserContext();
            taskData.getBuiltInMetrics().registerAll(topoConf, userContext);
            if (boltObject instanceof ICredentialsListener) {
                ((ICredentialsListener) boltObject).setCredentials(credentials);
            }
            if (Constants.SYSTEM_COMPONENT_ID.equals(componentId)) {
                Map<String, DisruptorQueue> map = ImmutableMap.of("sendqueue", transferQueue, "receive", receiveQueue,
                        "transfer", workerData.getTransferQueue());
                BuiltinMetricsUtil.registerQueueMetrics(map, topoConf, userContext);

                Map cachedNodePortToSocket = (Map) workerData.getCachedNodeToPortSocket().get();
                BuiltinMetricsUtil.registerIconnectionClientMetrics(cachedNodePortToSocket, topoConf, userContext);
                BuiltinMetricsUtil.registerIconnectionServerMetric(workerData.getReceiver(), topoConf, userContext);
            } else {
                Map<String, DisruptorQueue> map = ImmutableMap.of("sendqueue", transferQueue, "receive", receiveQueue);
                BuiltinMetricsUtil.registerQueueMetrics(map, topoConf, userContext);
            }

            IOutputCollector outputCollector = new BoltOutputCollectorImpl(this, taskData, entry.getKey(), rand, hasEventLoggers, isDebug);
            boltObject.prepare(topoConf, userContext, new OutputCollector(outputCollector));
        }
        openOrPrepareWasCalled.set(true);
        LOG.info("Prepared bolt {}:{}", componentId, idToTask.keySet());
        setupMetrics();
    }

    @Override
    public Callable<Object> call() throws Exception {
        init(idToTask);

        return new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                receiveQueue.consumeBatchWhenAvailable(BoltExecutor.this);
                return 0L;
            }
        };
    }

    @Override
    public void tupleActionFn(int taskId, TupleImpl tuple) throws Exception {
        String streamId = tuple.getSourceStreamId();
        if (Constants.CREDENTIALS_CHANGED_STREAM_ID.equals(streamId)) {
            Object taskObject = idToTask.get(taskId).getTaskObject();
            if (taskObject instanceof ICredentialsListener) {
                ((ICredentialsListener) taskObject).setCredentials((Map<String, String>) tuple.getValue(0));
            }
        } else if (Constants.METRICS_TICK_STREAM_ID.equals(streamId)) {
            metricsTick(idToTask.get(taskId), tuple);
        } else {
            IBolt boltObject = (IBolt) idToTask.get(taskId).getTaskObject();
            boolean isSampled = sampler.call();
            boolean isExecuteSampler = executeSampler.call();
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
            new BoltExecuteInfo(tuple, taskId, delta).applyOn(idToTask.get(taskId).getUserContext());
            if (delta >= 0) {
                ((BoltExecutorStats) stats).boltExecuteTuple(tuple.getSourceComponent(), tuple.getSourceStreamId(), delta);
            }
        }
    }

}
