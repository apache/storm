/*
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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.Thrift;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for custom metrics with FakeMetricConsumer.
 *
 * Ported from storm-core/test/clj/org/apache/storm/metrics_test.clj
 */
public class MetricsIntegrationTest {

    /**
     * Bolt that acks every tuple and increments a custom CountMetric.
     */
    static class CountAcksBolt extends BaseRichBolt {
        private OutputCollector collector;
        private CountMetric customMetric;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            this.customMetric = new CountMetric();
            context.registerMetric("my-custom-metric", customMetric, 5);
        }

        @Override
        public void execute(Tuple tuple) {
            customMetric.incr();
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }

    private static Map<String, Object> metricsConf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER,
            List.of(Map.of("class", "org.apache.storm.metric.FakeMetricConsumer")));
        conf.put("storm.zookeeper.connection.timeout", 30000);
        conf.put("storm.zookeeper.session.timeout", 60000);
        return conf;
    }

    private static void waitForAtLeastNBuckets(int n, String compId, String metricName,
                                                LocalCluster cluster) throws Exception {
        Testing.whileTimeout(Testing.TEST_TIMEOUT_MS,
            () -> {
                Map<Integer, Collection<Object>> taskIdToBuckets =
                    FakeMetricConsumer.getTaskIdToBuckets(compId, metricName);
                if (n != 0 && taskIdToBuckets == null) {
                    return true;
                }
                if (taskIdToBuckets == null) {
                    return false;
                }
                for (Collection<Object> buckets : taskIdToBuckets.values()) {
                    if (buckets.size() < n) {
                        return true;
                    }
                }
                return false;
            },
            () -> {
                try {
                    cluster.advanceClusterTime(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
    }

    private static List<Object> lookupBuckets(String compId, String metricName) {
        Map<Integer, Collection<Object>> taskIdToBuckets =
            FakeMetricConsumer.getTaskIdToBuckets(compId, metricName);
        if (taskIdToBuckets == null || taskIdToBuckets.isEmpty()) {
            return List.of();
        }
        Collection<Object> buckets = taskIdToBuckets.values().iterator().next();
        return List.copyOf(buckets);
    }

    private static void assertMetricRunningSum(String compId, String metricName,
                                                long expected, int minBuckets,
                                                LocalCluster cluster) throws Exception {
        waitForAtLeastNBuckets(minBuckets, compId, metricName, cluster);
        try {
            Awaitility.with()
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .conditionEvaluationListener(condition -> {
                    try {
                        cluster.advanceClusterTime(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                })
                .atMost(Testing.TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .until((Callable<Long>) () -> {
                    List<Object> buckets = lookupBuckets(compId, metricName);
                    long sum = 0;
                    for (Object b : buckets) {
                        sum += ((Number) b).longValue();
                    }
                    return sum;
                }, CoreMatchers.equalTo(expected));
        } catch (ConditionTimeoutException e) {
            throw new AssertionError(e.getMessage());
        }
    }

    @Test
    public void testCustomMetric() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withDaemonConf(metricsConf())
                .build()) {

            FeederSpout feeder = new FeederSpout(new Fields("field1"));
            var topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(feeder)),
                Map.of("2", Thrift.prepareBoltDetails(
                    Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareGlobalGrouping()),
                    new CountAcksBolt())));

            cluster.submitTopology("metrics-tester", Map.of(), topology);

            feeder.feed(List.of("a"), 1);
            cluster.advanceClusterTime(6);
            assertMetricRunningSum("2", "my-custom-metric", 1, 1, cluster);

            cluster.advanceClusterTime(5);
            assertMetricRunningSum("2", "my-custom-metric", 1, 2, cluster);

            cluster.advanceClusterTime(20);
            assertMetricRunningSum("2", "my-custom-metric", 1, 6, cluster);

            feeder.feed(List.of("b"), 2);
            feeder.feed(List.of("c"), 3);
            cluster.advanceClusterTime(5);
            assertMetricRunningSum("2", "my-custom-metric", 3, 7, cluster);
        }
    }

    @Test
    public void testCustomMetricWithMultiTasks() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withDaemonConf(metricsConf())
                .build()) {

            FeederSpout feeder = new FeederSpout(new Fields("field1"));
            Map<String, Object> boltConf = new HashMap<>();
            boltConf.put(Config.TOPOLOGY_TASKS, 2);

            var topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(feeder)),
                Map.of("2", Thrift.prepareBoltDetails(
                    Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareAllGrouping()),
                    new CountAcksBolt(), 1, boltConf)));

            cluster.submitTopology("metrics-tester-with-multitasks", Map.of(), topology);

            feeder.feed(List.of("a"), 1);
            cluster.advanceClusterTime(6);
            assertMetricRunningSum("2", "my-custom-metric", 1, 1, cluster);

            cluster.advanceClusterTime(5);
            assertMetricRunningSum("2", "my-custom-metric", 1, 2, cluster);

            cluster.advanceClusterTime(20);
            assertMetricRunningSum("2", "my-custom-metric", 1, 6, cluster);

            feeder.feed(List.of("b"), 2);
            feeder.feed(List.of("c"), 3);
            cluster.advanceClusterTime(5);
            assertMetricRunningSum("2", "my-custom-metric", 3, 7, cluster);
        }
    }
}
