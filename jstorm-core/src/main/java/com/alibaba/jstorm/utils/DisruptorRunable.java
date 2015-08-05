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

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.common.metric.Timer;
import com.alibaba.jstorm.metric.JStormHealthCheck;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.lmax.disruptor.EventHandler;

//import com.alibaba.jstorm.message.zeroMq.ISendConnection;

/**
 * 
 * Disruptor Consumer thread
 * 
 * @author yannian
 * 
 */
public abstract class DisruptorRunable extends RunnableCallback implements
        EventHandler {
    private final static Logger LOG = LoggerFactory
            .getLogger(DisruptorRunable.class);

    protected DisruptorQueue queue;
    protected String idStr;
    protected Timer timer;
    protected AtomicBoolean shutdown = AsyncLoopRunnable.getShutdown();

    public DisruptorRunable(DisruptorQueue queue, String idStr) {
        this.queue = queue;
        this.timer =
                JStormMetrics.registerWorkerTimer(idStr + MetricDef.TIME_TYPE);
        this.idStr = idStr;

        QueueGauge queueGauge =
                new QueueGauge(idStr + MetricDef.QUEUE_TYPE, queue);
        JStormMetrics.registerWorkerGauge(queueGauge, idStr
                + MetricDef.QUEUE_TYPE);

        JStormHealthCheck.registerWorkerHealthCheck(idStr, queueGauge);
    }

    public abstract void handleEvent(Object event, boolean endOfBatch)
            throws Exception;

    /**
     * This function need to be implements
     * 
     * @see com.lmax.disruptor.EventHandler#onEvent(java.lang.Object, long,
     *      boolean)
     */
    @Override
    public void onEvent(Object event, long sequence, boolean endOfBatch)
            throws Exception {
        if (event == null) {
            return;
        }

        long start = System.nanoTime();
        try {
            handleEvent(event, endOfBatch);
        } finally {
            long end = System.nanoTime();
            timer.update((end - start)/1000000.0d);
        }
    }

    @Override
    public void run() {
        LOG.info("Successfully start thread " + idStr);
        queue.consumerStarted();

        while (shutdown.get() == false) {
            queue.consumeBatchWhenAvailable(this);

        }

        LOG.info("Successfully exit thread " + idStr);
    }

    @Override
    public void shutdown() {
        JStormMetrics.unregisterWorkerMetric(idStr + MetricDef.QUEUE_TYPE);
        JStormHealthCheck.unregisterWorkerHealthCheck(idStr);
    }

}
