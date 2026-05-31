/*
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

package org.apache.storm.starter;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.storm.Config;
import org.apache.storm.grouping.JitterAwareStreamGrouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Example wiring of {@link JitterAwareStreamGrouping}.
 *
 * <p>The {@code source} bolt emits to the {@code worker} bolt through a {@link JitterAwareStreamGrouping}:
 * each tuple is routed to the {@code worker} task that currently reports the lowest jitter. The jitter
 * figures travel back to {@code source} as upstream feedback, which must be enabled on the topology via
 * {@link Config#TOPOLOGY_UPSTREAM_FEEDBACK_ENABLE}. The {@code worker} bolt below injects an artificial,
 * uneven processing delay so the per-task jitter actually differs and the grouping has something to
 * optimize; in a real topology the variance comes from the bolt's own work.</p>
 *
 * <p>Until feedback data has accumulated (and on any task that has not reported yet) the grouping falls
 * back to round-robin, so the topology behaves like an even spread at start-up.</p>
 */
public class JitterAwareGroupingTopology extends ConfigurableTopology {

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new JitterAwareGroupingTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("words", new TestWordSpout(), 2);
        builder.setBolt("source", new ForwardBolt(), 2).shuffleGrouping("words");
        // Route source -> worker by lowest reported jitter instead of shuffle/fields.
        builder.setBolt("worker", new JitteryBolt(), 4)
               .customGrouping("source", new JitterAwareStreamGrouping());

        // Required: jitter feedback must flow from the children (worker) back to the parents (source).
        conf.put(Config.TOPOLOGY_STATS_EWMA_ENABLE, true);
        conf.put(Config.TOPOLOGY_UPSTREAM_FEEDBACK_ENABLE, true);
        conf.put(Config.TOPOLOGY_UPSTREAM_FEEDBACK_FREQ_SECS, 2);

        conf.setNumWorkers(2);

        String topologyName = "jitter-aware-grouping";
        if (args != null && args.length > 0) {
            topologyName = args[0];
        }
        return submit(topologyName, conf, builder);
    }

    /** Passes each word straight through to the jitter-aware stream. */
    public static class ForwardBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            collector.emit(tuple, new Values(tuple.getString(0)));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    /** Simulates uneven per-task processing time so tasks accrue different jitter. */
    public static class JitteryBolt extends BaseRichBolt {
        private OutputCollector collector;
        private long baseDelayMicros;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            // Give each task its own baseline latency so their jitter profiles diverge.
            this.baseDelayMicros = 50L * context.getThisTaskIndex();
        }

        @Override
        public void execute(Tuple tuple) {
            long jitterMicros = ThreadLocalRandom.current().nextLong(0, 200);
            busyWaitMicros(baseDelayMicros + jitterMicros);
            collector.ack(tuple);
        }

        private static void busyWaitMicros(long micros) {
            long deadline = System.nanoTime() + micros * 1_000L;
            while (System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // terminal bolt: no output
        }
    }
}
