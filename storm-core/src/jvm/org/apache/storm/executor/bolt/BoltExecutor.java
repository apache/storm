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
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.metrics.BuiltinMetricsUtil;
import org.apache.storm.executor.BaseExecutor;
import org.apache.storm.executor.ExecutorData;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public class BoltExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(BoltExecutor.class);

    private final Callable<Boolean> executeSampler;

    public BoltExecutor(ExecutorData executorData, Map<Integer, Task> taskDatas, Map<String, String> credentials) {
        super(executorData, taskDatas, credentials);
        this.executeSampler = ConfigUtils.mkStatsSampler(stormConf);
        init();
    }

    @Override
    protected void init() {
        while (!executorData.getStormActiveAtom().get()) {
            Utils.sleep(100);
        }
        LOG.info("Preparing bolt {}:{}", componentId, taskDatas.keySet());
        for (Map.Entry<Integer, Task> entry : taskDatas.entrySet()) {
            Task taskData = entry.getValue();
            IBolt boltObject = (IBolt) taskData.getTaskObject();
            TopologyContext userContext = taskData.getUserContext();
            taskData.getBuiltInMetrics().registerAll(stormConf, userContext);
            if (boltObject instanceof ICredentialsListener) {
                ((ICredentialsListener) boltObject).setCredentials(credentials);
            }
            if (componentId == Constants.SYSTEM_COMPONENT_ID) {
                Map<String, DisruptorQueue> map = ImmutableMap.of("sendqueue", transferQueue, "receive", receiveQueue, "transfer",
                        (DisruptorQueue) executorData.getWorkerData().get("transfer"));
                BuiltinMetricsUtil.registerQueueMetrics(map, stormConf, userContext);
                Map cachedNodePortToSocket = (Map) ((AtomicReference) executorData.getWorkerData().get("cached-node+port->socket")).get();
                BuiltinMetricsUtil.registerIconnectionClientMetrics(cachedNodePortToSocket, stormConf, userContext);
                BuiltinMetricsUtil.registerQueueMetrics((Map) executorData.getWorkerData().get("receiver"), stormConf, userContext);
            } else {
                Map<String, DisruptorQueue> map = ImmutableMap.of("sendqueue", transferQueue, "receive", receiveQueue);
                BuiltinMetricsUtil.registerQueueMetrics(map, stormConf, userContext);
            }

            IOutputCollector outputCollector = new BoltOutputCollectorImpl(executorData, taskData, entry.getKey(), rand, isEventLoggers);

            boltObject.prepare(stormConf, userContext, new OutputCollector(outputCollector));
        }
        executorData.setOpenOrprepareWasCalled(true);
        LOG.info("Opened bolt {}:{}", componentId, taskDatas.keySet());
        setupMetrics();
    }

    @Override
    public Object call() throws Exception {
        receiveQueue.consumeBatchWhenAvailable(this);
        return (long) 0;
    }

    @Override
    public void tupleActionFn(int taskId, TupleImpl tuple) throws Exception{
        String streamId = tuple.getSourceStreamId();
        if (streamId == Constants.CREDENTIALS_CHANGED_STREAM_ID) {
            Object taskObject = taskDatas.get(taskId).getTaskObject();
            if (taskObject instanceof ICredentialsListener) {
                ((ICredentialsListener) taskObject).setCredentials((Map<String, String>) tuple.getValue(0));
            }
        } else if (streamId == Constants.METRICS_TICK_STREAM_ID) {
            metricsTick(taskDatas.get(taskId), tuple);
        } else {
            IBolt boltObject = (IBolt) taskDatas.get(taskId).getTaskObject();
            boolean isSampler = sampler.call();
            boolean isExecuteSampler = executeSampler.call();
            long now = (isSampler || isExecuteSampler) ? System.currentTimeMillis() : 0;
            if (isSampler)
                tuple.setProcessSampleStartTime(now);
            if (isExecuteSampler)
                tuple.setProcessSampleStartTime(now);
            boltObject.execute(tuple);

            Long ms = tuple.getExecuteSampleStartTime();
            long delta = (ms != null) ? Time.deltaMs(ms) : 0;
            if (Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false)) {
                LOG.info("Execute done TUPLE {} TASK: {} DELTA: {}", tuple, taskId, delta);
            }
            new BoltExecuteInfo(tuple, taskId, delta).applyOn(taskDatas.get(taskId).getUserContext());
            if (delta != 0) {
                ((BoltExecutorStats) executorData.getStats()).boltExecuteTuple(tuple.getSourceComponent(), tuple.getSourceStreamId(), delta);
            }
        }
    }

}
