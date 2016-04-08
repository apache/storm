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

import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class MetricsConsumerBolt implements IBolt {
    public static final Logger LOG = LoggerFactory.getLogger(MetricsConsumerBolt.class);

    IMetricsConsumer _metricsConsumer;
    String _consumerClassName;
    OutputCollector _collector;
    Object _registrationArgument;

    private final BlockingQueue<MetricsTask> _taskQueue = new LinkedBlockingDeque<>();
    private Thread _taskExecuteThread;
    private volatile boolean _running = true;

    public MetricsConsumerBolt(String consumerClassName, Object registrationArgument) {
        _consumerClassName = consumerClassName;
        _registrationArgument = registrationArgument;
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
        _taskQueue.add(new MetricsTask((IMetricsConsumer.TaskInfo)input.getValue(0), (Collection)input.getValue(1)));
        _collector.ack(input);
    }

    @Override
    public void cleanup() {
        _running = false;
        _metricsConsumer.cleanup();
        _taskExecuteThread.interrupt();
    }

    class MetricsTask {
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
