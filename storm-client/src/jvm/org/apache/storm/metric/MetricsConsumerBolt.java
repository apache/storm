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

package org.apache.storm.metric;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.metric.util.DataPointExpander;
import org.apache.storm.shade.com.google.common.base.Predicate;
import org.apache.storm.shade.com.google.common.collect.Iterables;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsConsumerBolt implements IBolt {
    public static final Logger LOG = LoggerFactory.getLogger(MetricsConsumerBolt.class);
    private final int maxRetainMetricTuples;
    private final Predicate<IMetricsConsumer.DataPoint> filterPredicate;
    private final DataPointExpander expander;
    private final BlockingQueue<MetricsTask> taskQueue;
    IMetricsConsumer metricsConsumer;
    String consumerClassName;
    OutputCollector collector;
    Object registrationArgument;
    private Thread taskExecuteThread;
    private volatile boolean running = true;

    public MetricsConsumerBolt(String consumerClassName, Object registrationArgument, int maxRetainMetricTuples,
                               Predicate<IMetricsConsumer.DataPoint> filterPredicate, DataPointExpander expander) {

        this.consumerClassName = consumerClassName;
        this.registrationArgument = registrationArgument;
        this.maxRetainMetricTuples = maxRetainMetricTuples;
        this.filterPredicate = filterPredicate;
        this.expander = expander;

        if (this.maxRetainMetricTuples > 0) {
            taskQueue = new LinkedBlockingDeque<>(this.maxRetainMetricTuples);
        } else {
            taskQueue = new LinkedBlockingDeque<>();
        }
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        try {
            metricsConsumer = (IMetricsConsumer) Class.forName(consumerClassName).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate a class listed in config under section "
                            + Config.TOPOLOGY_METRICS_CONSUMER_REGISTER
                            + " with fully qualified name "
                            + consumerClassName,
                    e);
        }
        metricsConsumer.prepare(topoConf, registrationArgument, context, collector);
        this.collector = collector;
        taskExecuteThread = new Thread(new MetricsHandlerRunnable());
        taskExecuteThread.setDaemon(true);
        taskExecuteThread.start();
    }

    @Override
    public void execute(Tuple input) {
        IMetricsConsumer.TaskInfo taskInfo = (IMetricsConsumer.TaskInfo) input.getValue(0);
        Collection<IMetricsConsumer.DataPoint> dataPoints = (Collection) input.getValue(1);
        Collection<IMetricsConsumer.DataPoint> expandedDataPoints = expander.expandDataPoints(dataPoints);
        List<IMetricsConsumer.DataPoint> filteredDataPoints = getFilteredDataPoints(expandedDataPoints);
        MetricsTask metricsTask = new MetricsTask(taskInfo, filteredDataPoints);

        while (!taskQueue.offer(metricsTask)) {
            taskQueue.poll();
        }

        collector.ack(input);
    }

    private List<IMetricsConsumer.DataPoint> getFilteredDataPoints(Collection<IMetricsConsumer.DataPoint> dataPoints) {
        return Lists.newArrayList(Iterables.filter(dataPoints, filterPredicate));
    }

    @Override
    public void cleanup() {
        running = false;
        metricsConsumer.cleanup();
        taskExecuteThread.interrupt();
    }

    static class MetricsTask {
        private IMetricsConsumer.TaskInfo taskInfo;
        private Collection<IMetricsConsumer.DataPoint> dataPoints;

        MetricsTask(IMetricsConsumer.TaskInfo taskInfo, Collection<IMetricsConsumer.DataPoint> dataPoints) {
            this.taskInfo = taskInfo;
            this.dataPoints = dataPoints;
        }

        public IMetricsConsumer.TaskInfo getTaskInfo() {
            return taskInfo;
        }

        public Collection<IMetricsConsumer.DataPoint> getDataPoints() {
            return dataPoints;
        }
    }

    class MetricsHandlerRunnable implements Runnable {

        @Override
        public void run() {
            while (running) {
                try {
                    MetricsTask task = taskQueue.take();
                    metricsConsumer.handleDataPoints(task.getTaskInfo(), task.getDataPoints());
                } catch (InterruptedException e) {
                    break;
                } catch (Throwable t) {
                    LOG.error("Exception occurred during handle metrics", t);
                }
            }
        }
    }

}
