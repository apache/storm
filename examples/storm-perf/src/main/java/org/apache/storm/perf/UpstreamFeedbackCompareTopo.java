/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.perf;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.JitterAwareStreamGrouping;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * Benchmark to compare the effect of {@link Config#TOPOLOGY_UPSTREAM_FEEDBACK_ENABLE}.
 *
 * <p>Pipeline (acked, so jitter feedback can flow): {@code gen -> source -> worker -> sink}. The
 * {@code source -> worker} edge uses {@link JitterAwareStreamGrouping}, and each {@code worker} task adds
 * an uneven, task-dependent processing delay so the per-task jitter diverges. With feedback enabled the
 * grouping steers tuples toward the lower-jitter workers; with it disabled the grouping has no stats and
 * falls back to round-robin.</p>
 *
 * <p>The {@code worker} bolt must emit (anchored) for its jitter to be reported back to {@code source};
 * that is why a terminal {@code sink} bolt sits downstream. Feedback also requires the EWMA jitter gauges,
 * which this topology enables via {@link Config#TOPOLOGY_STATS_EWMA_ENABLE}.</p>
 *
 * <p>Run the same topology twice and compare the throughput/latency printed by the perf framework:</p>
 * <pre>
 *   storm jar storm-perf.jar org.apache.storm.perf.UpstreamFeedbackCompareTopo 120 -c topology.upstream.feedback.enable=false
 *   storm jar storm-perf.jar org.apache.storm.perf.UpstreamFeedbackCompareTopo 120 -c topology.upstream.feedback.enable=true
 * </pre>
 */
public class UpstreamFeedbackCompareTopo {

    static final String SPOUT_ID = "gen";
    static final String SOURCE_ID = "source";
    static final String WORKER_ID = "worker";
    static final String SINK_ID = "sink";

    // Parallelism / tuning keys (override with -c <key>=<value>).
    static final String SPOUT_NUM = "spout.count";
    static final String SOURCE_NUM = "source.count";
    static final String WORKER_NUM = "worker.count";
    static final String SINK_NUM = "sink.count";
    static final String WORKER_BASE_DELAY_US = "worker.base.delay.us";

    private static final String FIELD = "t";

    static StormTopology getTopology(Map<String, Object> conf) {
        int spouts = Helper.getInt(conf, SPOUT_NUM, 1);
        int sources = Helper.getInt(conf, SOURCE_NUM, 2);
        int workers = Helper.getInt(conf, WORKER_NUM, 4);
        int sinks = Helper.getInt(conf, SINK_NUM, 1);
        long baseDelayUs = Helper.getInt(conf, WORKER_BASE_DELAY_US, 100);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new GenSpout(), spouts);
        builder.setBolt(SOURCE_ID, new ForwardBolt(), sources).localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(WORKER_ID, new JitteryBolt(baseDelayUs), workers)
               .customGrouping(SOURCE_ID, new JitterAwareStreamGrouping());
        builder.setBolt(SINK_ID, new SinkBolt(), sinks).localOrShuffleGrouping(WORKER_ID);
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        int runTime = -1;
        Config topoConf = new Config();
        if (args.length > 0) {
            runTime = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            topoConf.putAll(Utils.findAndReadConfigFile(args[1]));
        }
        if (args.length > 2) {
            System.err.println("args: [runDurationSec]  [optionalConfFile]");
            return;
        }

        // EWMA jitter gauges feed the feedback signal; enable them regardless of the toggle under test.
        topoConf.put(Config.TOPOLOGY_STATS_EWMA_ENABLE, true);
        topoConf.putIfAbsent(Config.TOPOLOGY_UPSTREAM_FEEDBACK_RATIO, 0.01);
        topoConf.putIfAbsent(Config.TOPOLOGY_MAX_SPOUT_PENDING, 2000);
        // TOPOLOGY_UPSTREAM_FEEDBACK_ENABLE is left to the command line so the two runs differ only by it.
        topoConf.putAll(Utils.readCommandLineOpts());

        Helper.runOnClusterAndPrintMetrics(runTime, "UpstreamFeedbackCompareTopo", topoConf, getTopology(topoConf));
    }

    /** Emits a timestamp per tuple, anchored (with a msgId) so the chain is acked end to end. */
    private static class GenSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private long msgId;

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            collector.emit(new Values(System.currentTimeMillis()), ++msgId);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELD));
        }
    }

    /** Forwards each tuple, anchored, onto the jitter-aware stream. */
    private static class ForwardBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            collector.emit(tuple, new Values(tuple.getValue(0)));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELD));
        }
    }

    /** Adds an uneven, per-task processing delay, then emits anchored so its jitter is reported upstream. */
    private static class JitteryBolt extends BaseRichBolt {
        private final long baseDelayUs;
        private OutputCollector collector;
        private long taskBaselineUs;

        JitteryBolt(long baseDelayUs) {
            this.baseDelayUs = baseDelayUs;
        }

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            // Each task gets its own baseline latency so the workers' jitter profiles differ.
            this.taskBaselineUs = baseDelayUs * context.getThisTaskIndex();
        }

        @Override
        public void execute(Tuple tuple) {
            busyWaitMicros(taskBaselineUs + ThreadLocalRandom.current().nextLong(0, baseDelayUs + 1));
            collector.emit(tuple, new Values(tuple.getValue(0)));
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
            declarer.declare(new Fields(FIELD));
        }
    }

    /** Terminal bolt: acks to complete the tuple tree (drives spout complete-latency). */
    private static class SinkBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // terminal
        }
    }
}
