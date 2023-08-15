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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metrics2.PerReporterGauge;
import org.apache.storm.metrics2.WorkerMetricRegistrant;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;

// There is one task inside one executor for each worker of the topology.
// TaskID is always -1, therefore you can only send-unanchored tuples to co-located SystemBolt.
// This bolt was conceived to export worker stats via metrics api.
public class SystemBolt implements IBolt {
    private static boolean prepareWasCalled = false;

    @SuppressWarnings({ "unchecked" })
    @Override
    public void prepare(final Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        if (prepareWasCalled && !"local".equals(topoConf.get(Config.STORM_CLUSTER_MODE))) {
            throw new RuntimeException("A single worker should have 1 SystemBolt instance.");
        }
        prepareWasCalled = true;

        context.registerMetricSet("GC", new GarbageCollectorMetricSet());
        context.registerMetricSet("threads", new ThreadStatesGaugeSet());
        context.registerMetricSet("memory", new MemoryUsageGaugeSet());

        final RuntimeMXBean jvmRt = ManagementFactory.getRuntimeMXBean();

        context.registerGauge("uptimeSecs", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return jvmRt.getUptime() / 1000L;
            }
        });

        context.registerGauge("startTimeSecs", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return jvmRt.getStartTime() / 1000L;
            }
        });


        context.registerGauge("newWorkerEvent", new NewWorkerGauge());

        int bucketSize = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        registerMetrics(context, (Map<String, String>) topoConf.get(Config.WORKER_METRICS), bucketSize, topoConf);
        registerMetrics(context, (Map<String, String>) topoConf.get(Config.TOPOLOGY_WORKER_METRICS), bucketSize, topoConf);
    }

    // newWorkerEvent: 1 when a worker is first started and 0 all other times.
    // This can be used to tell when a worker has crashed and is restarted.
    private class NewWorkerMetric {
        boolean newWorker = true;

        public int getValueAndReset() {
            if (newWorker) {
                newWorker = false;
                return 1;
            } else {
                return 0;
            }
        }
    }

    // allow reporting new worker metric for multiple reporters if they support getValueForReporter().
    private class NewWorkerGauge extends PerReporterGauge<Integer> {
        private final NewWorkerMetric defaultValue = new NewWorkerMetric();
        private final Map<Object, NewWorkerMetric> reporterValues = new HashMap<>();

        // reporting this value will not be accurate for multiple reporters
        @Override
        public Integer getValue() {
            return (Integer) defaultValue.getValueAndReset();
        }

        @Override
        public Integer getValueForReporter(Object reporter) {
            return (Integer) reporterValues.computeIfAbsent(reporter, (rep) -> new NewWorkerMetric()).getValueAndReset();
        }
    }

    private void registerMetrics(TopologyContext context, Map<String, String> metrics, int bucketSize, Map<String, Object> conf) {
        if (metrics == null) {
            return;
        }
        for (Map.Entry<String, String> metric : metrics.entrySet()) {
            try {
                Object workerMetric = ReflectionUtils.newInstance(metric.getValue(), conf);
                if (workerMetric instanceof IMetric) {
                    context.registerMetric(metric.getKey(), (IMetric) workerMetric, bucketSize);
                } else if (workerMetric instanceof WorkerMetricRegistrant) {
                    ((WorkerMetricRegistrant) workerMetric).registerMetrics(context);
                } else {
                    throw new RuntimeException("Invalid worker metric " + workerMetric);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void execute(Tuple input) {
        throw new RuntimeException("Non-system tuples should never be sent to __system bolt.");
    }

    @Override
    public void cleanup() {
    }
}
