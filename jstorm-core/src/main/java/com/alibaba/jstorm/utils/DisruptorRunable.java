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
package com.alibaba.jstorm.utils;

import backtype.storm.utils.DisruptorQueue;
import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.common.metric.*;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.metric.JStormHealthCheck;
import com.alibaba.jstorm.metric.MetricDef;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

//import com.alibaba.jstorm.message.zeroMq.ISendConnection;

/**
 * Disruptor Consumer thread
 * 
 * @author yannian
 */
public abstract class DisruptorRunable extends RunnableCallback implements EventHandler {
    private final static Logger LOG = LoggerFactory.getLogger(DisruptorRunable.class);

    protected DisruptorQueue queue;
    protected String idStr;
    protected AsmHistogram timer;
    protected AtomicBoolean shutdown = AsyncLoopRunnable.getShutdown();

    public DisruptorRunable(DisruptorQueue queue, String idStr) {
        this.queue = queue;
        this.idStr = idStr;

        this.timer =
                (AsmHistogram) JStormMetrics.registerWorkerMetric(MetricUtils.workerMetricName(idStr + MetricDef.TIME_TYPE, MetricType.HISTOGRAM),
                        new AsmHistogram());

        QueueGauge queueGauge = new QueueGauge(queue, idStr, MetricDef.QUEUE_TYPE);
        JStormMetrics.registerWorkerMetric(MetricUtils.workerMetricName(idStr + MetricDef.QUEUE_TYPE, MetricType.GAUGE), new AsmGauge(queueGauge));

        JStormHealthCheck.registerWorkerHealthCheck(idStr, queueGauge);
    }

    public abstract void handleEvent(Object event, boolean endOfBatch) throws Exception;

    /**
     * This function need to be implements
     * 
     * @see EventHandler#onEvent(Object, long, boolean)
     */
    @Override
    public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
        if (event == null) {
            return;
        }

        long start = System.nanoTime();
        try {
            handleEvent(event, endOfBatch);
        } finally {
            long end = System.nanoTime();
            timer.update((end - start) / TimeUtils.NS_PER_US);
        }
    }

    @Override
    public void run() {
        LOG.info("Successfully start thread " + idStr);
        queue.consumerStarted();

        while (!shutdown.get()) {
            queue.consumeBatchWhenAvailable(this);
        }
        LOG.info("Successfully exit thread " + idStr);
    }

    @Override
    public void shutdown() {
        JStormMetrics.unregisterWorkerMetric(MetricUtils.workerMetricName(idStr + MetricDef.QUEUE_TYPE, MetricType.GAUGE));
        JStormHealthCheck.unregisterWorkerHealthCheck(idStr);
    }

}
