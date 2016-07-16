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
package org.apache.storm.metric;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.metric.util.DataPointExpander;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class MetricsConsumerBolt implements IBolt {
    public static final Logger LOG = LoggerFactory.getLogger(MetricsConsumerBolt.class);

    IMetricsConsumer _metricsConsumer;
    String _consumerClassName;
    OutputCollector _collector;
    Object _registrationArgument;
    private final int _maxRetainMetricTuples;
    private final Predicate<IMetricsConsumer.DataPoint> _filterPredicate;
    private final DataPointExpander _expander;

    private final BlockingQueue<MetricsTask> _taskQueue;
    private Thread _taskExecuteThread;
    private volatile boolean _running = true;

    public MetricsConsumerBolt(String consumerClassName, Object registrationArgument, int maxRetainMetricTuples,
                               Predicate<IMetricsConsumer.DataPoint> filterPredicate, DataPointExpander expander) {

        _consumerClassName = consumerClassName;
        _registrationArgument = registrationArgument;
        _maxRetainMetricTuples = maxRetainMetricTuples;
        _filterPredicate = filterPredicate;
        _expander = expander;

        if (_maxRetainMetricTuples > 0) {
            _taskQueue = new LinkedBlockingDeque<>(_maxRetainMetricTuples);
        } else {
            _taskQueue = new LinkedBlockingDeque<>();
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            _metricsConsumer = (IMetricsConsumer)Class.forName(_consumerClassName).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate a class listed in config under section " +
                Config.TOPOLOGY_METRICS_CONSUMER_REGISTER + " with fully qualified name " + _consumerClassName, e);
        }
        _metricsConsumer.prepare(stormConf, _registrationArgument, context, collector);
        _collector = collector;
        _taskExecuteThread = new Thread(new MetricsHandlerRunnable());
        _taskExecuteThread.setDaemon(true);
        _taskExecuteThread.start();
    }
    
    @Override
    public void execute(Tuple input) {
        IMetricsConsumer.TaskInfo taskInfo = (IMetricsConsumer.TaskInfo) input.getValue(0);
        Collection<IMetricsConsumer.DataPoint> dataPoints = (Collection) input.getValue(1);
        Collection<IMetricsConsumer.DataPoint> expandedDataPoints = _expander.expandDataPoints(dataPoints);
        List<IMetricsConsumer.DataPoint> filteredDataPoints = getFilteredDataPoints(expandedDataPoints);
        MetricsTask metricsTask = new MetricsTask(taskInfo, filteredDataPoints);

        while (! _taskQueue.offer(metricsTask)) {
            _taskQueue.poll();
        }

        _collector.ack(input);
    }

    private List<IMetricsConsumer.DataPoint> getFilteredDataPoints(Collection<IMetricsConsumer.DataPoint> dataPoints) {
        return Lists.newArrayList(Iterables.filter(dataPoints, _filterPredicate));
    }

    @Override
    public void cleanup() {
        _running = false;
        _metricsConsumer.cleanup();
        _taskExecuteThread.interrupt();
    }

    static class MetricsTask {
        private IMetricsConsumer.TaskInfo taskInfo;
        private Collection<IMetricsConsumer.DataPoint> dataPoints;

        public MetricsTask(IMetricsConsumer.TaskInfo taskInfo, Collection<IMetricsConsumer.DataPoint> dataPoints) {
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
            while (_running) {
                try {
                    MetricsTask task = _taskQueue.take();
                    _metricsConsumer.handleDataPoints(task.getTaskInfo(), task.getDataPoints());
                } catch (InterruptedException e) {
                    break;
                } catch (Throwable t) {
                    LOG.error("Exception occurred during handle metrics", t);
                }
            }
        }
    }

}
