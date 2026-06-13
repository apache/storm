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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.grouping.JitterAwareStreamGrouping;
import org.apache.storm.grouping.LoadAwareShuffleGrouping;
import org.apache.storm.perf.spout.FileReadSpout;
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
 * Benchmark for {@link JitterAwareStreamGrouping} in a word-count pipeline where worker tasks have
 * artificially skewed latency <i>dispersion</i> (jitter).
 *
 * <p>Pipeline: {@code GenSpout -> SplitterBolt -> JitteryWorkerBolt -> SinkBolt}
 *
 * <p>{@code JitteryWorkerBolt} tasks have task-index-dependent latency <i>jitter</i>: task 0 is
 * perfectly steady and each higher index is progressively jitterier, because the per-tuple noise
 * <i>width</i> grows with the task index over a constant floor. This deliberately exercises the signal
 * the grouping ranks on — RFC-1889 jitter (the EWMA of {@code |Δlatency|}), which measures dispersion,
 * not level. A per-task <i>mean</i> offset would not work: it cancels in the estimator's
 * consecutive-difference, leaving every task with identical jitter. With upstream feedback enabled,
 * {@link JitterAwareStreamGrouping} steers more tuples toward the steadiest (lowest-jitter) tasks,
 * improving throughput and reducing complete latency compared to a load-aware shuffle baseline.
 *
 * <p>Run the baseline and the jitter-aware run back-to-back to compare. Select the grouping with the
 * {@code grouping.mode} flag:
 * <pre>
 *   # Baseline: plain LoadAwareShuffleGrouping (no upstream feedback needed).
 *   storm jar storm-perf.jar org.apache.storm.perf.JitterAwareGroupingTopology 120 \
 *       -c grouping.mode=loadaware
 *
 *   # Jitter-aware: feedback-driven grouping steers tuples to lowest-jitter workers (default mode).
 *   storm jar storm-perf.jar org.apache.storm.perf.JitterAwareGroupingTopology 120 \
 *       -c grouping.mode=jitter
 * </pre>
 *
 * <p>Tuning knobs (pass with {@code -c key=value}):
 * <ul>
 *   <li>{@code grouping.mode} — {@code jitter} (default) or {@code loadaware} baseline</li>
 *   <li>{@code spout.count} — number of spout tasks (default 1)</li>
 *   <li>{@code splitter.count} — number of splitter tasks (default 2)</li>
 *   <li>{@code worker.count} — number of jittery worker tasks (default 4)</li>
 *   <li>{@code sink.count} — number of sink tasks (default 1)</li>
 *   <li>{@code input.file} — path to a text file whose lines are treated as sentences
 *       (e.g. {@code src/main/sampledata/randomwords.txt})</li>
 *   <li>{@code worker.base.delay.us} — jitter-width step in µs (default 2000). Task {@code i} parks for
 *       a constant {@code base} µs floor plus uniform noise in {@code [0, i * base]} µs, so the noise
 *       width (hence jitter) scales with the task index while the floor cancels in the jitter estimate.
 *       Must be &ge;1000 µs so the EWMA jitter gauge (millisecond resolution) sees distinct values per
 *       task.</li>
 * </ul>
 */
public class JitterAwareGroupingTopology {

    public static final String TOPOLOGY_NAME = "JitterAwareGroupingTopology";

    static final String SPOUT_ID = "gen";
    static final String SPLITTER_ID = "splitter";
    static final String WORKER_ID = "worker";
    static final String SINK_ID = "sink";

    static final String SPOUT_NUM = "spout.count";
    static final String SPLITTER_NUM = "splitter.count";
    static final String WORKER_NUM = "worker.count";
    static final String SINK_NUM = "sink.count";
    static final String INPUT_FILE = "input.file";
    static final String WORKER_BASE_DELAY_US = "worker.base.delay.us";

    static final String GROUPING_MODE = "grouping.mode";
    static final String MODE_JITTER = "jitter";
    static final String MODE_LOADAWARE = "loadaware";

    private static final String FIELD_SENTENCE = "sentence";
    private static final String FIELD_WORD = "word";
    private static final String FIELD_COUNT = "count";

    static StormTopology getTopology(Map<String, Object> conf) {
        int spouts = Helper.getInt(conf, SPOUT_NUM, 1);
        int splitters = Helper.getInt(conf, SPLITTER_NUM, 2);
        int workers = Helper.getInt(conf, WORKER_NUM, 4);
        int sinks = Helper.getInt(conf, SINK_NUM, 1);
        long baseDelayUs = Helper.getInt(conf, WORKER_BASE_DELAY_US, 2000);
        String inputFile = Helper.getStr(conf, INPUT_FILE);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new GenSpout(inputFile), spouts);
        builder.setBolt(SPLITTER_ID, new SplitterBolt(), splitters)
               .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(WORKER_ID, new JitteryWorkerBolt(baseDelayUs), workers)
               .customGrouping(SPLITTER_ID, selectGrouping(conf));
        builder.setBolt(SINK_ID, new SinkBolt(), sinks)
               .localOrShuffleGrouping(WORKER_ID);

        return builder.createTopology();
    }

    /**
     * Picks the {@code splitter -> worker} grouping from {@link #GROUPING_MODE}. Defaults to the
     * feedback-driven {@link JitterAwareStreamGrouping}; {@code grouping.mode=loadaware} selects the plain
     * {@link LoadAwareShuffleGrouping} baseline so the two can be benchmarked back-to-back. Both implement
     * {@link org.apache.storm.grouping.LoadAwareCustomStreamGrouping} and require the locality-aware
     * configuration set in {@link #main}.
     */
    static CustomStreamGrouping selectGrouping(Map<String, Object> conf) {
        String mode = Helper.getStr(conf, GROUPING_MODE);
        if (MODE_LOADAWARE.equalsIgnoreCase(mode)) {
            return new LoadAwareShuffleGrouping();
        }
        return new JitterAwareStreamGrouping();
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

        topoConf.put(Config.TOPOLOGY_STATS_EWMA_ENABLE, true);
        // max.spout.pending counts SPOUT tuples (sentences), but each fans out to ~7 word-tuples at the
        // SplitterBolt, so the in-flight backlog at the slow workers is ~7x this number. Keep it low enough
        // that steady-state complete latency stays well under topology.message.timeout.secs with ~0 fails;
        // 4000 oversubscribed the synthetic workers and pinned latency at the timeout. Tune per cluster:
        // complete_latency ~= (pending * fanout) / aggregate_worker_ack_rate (Little's law).
        topoConf.putIfAbsent(Config.TOPOLOGY_MAX_SPOUT_PENDING, 500);
        topoConf.putIfAbsent(Config.TOPOLOGY_UPSTREAM_FEEDBACK_FREQ_SECS, 10);

        // Both grouping modes resolve to a LoadAwareShuffleGrouping (directly, or as the jitter grouping's
        // fallback), whose prepare() requires these locality-aware keys. Normally supplied by defaults.yaml;
        // set defensively so the topology is self-contained. Placed before the CLI merge so -c can override.
        topoConf.putIfAbsent(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN,
            "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping");
        topoConf.putIfAbsent(Config.TOPOLOGY_LOCALITYAWARE_HIGHER_BOUND, 0.8);
        topoConf.putIfAbsent(Config.TOPOLOGY_LOCALITYAWARE_LOWER_BOUND, 0.2);

        topoConf.putAll(Utils.readCommandLineOpts());

        Helper.runOnClusterAndPrintMetrics(runTime, TOPOLOGY_NAME, topoConf, getTopology(topoConf));
    }

    /**
     * Emits anchored sentences loaded from {@code input.file} at maximum rate. The file is read
     * once into memory during {@link #open} and replayed in a round-robin loop. Anchoring (with a
     * msgId) ensures Storm tracks each tuple tree to completion, so spout complete-latency is a
     * reliable end-to-end signal.
     */
    private static class GenSpout extends BaseRichSpout {
        private final String filePath;
        private SpoutOutputCollector collector;
        private List<String> lines;
        private int lineIdx;
        private long msgId;

        GenSpout(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            try {
                this.lines = FileReadSpout.readLines(new FileInputStream(filePath));
            } catch (IOException e) {
                throw new RuntimeException("Cannot open input file: " + filePath, e);
            }
            if (lines.isEmpty()) {
                throw new RuntimeException("Input file is empty: " + filePath);
            }
        }

        @Override
        public void nextTuple() {
            String sentence = lines.get(lineIdx++ % lines.size());
            collector.emit(new Values(sentence), ++msgId);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELD_SENTENCE));
        }
    }

    /**
     * Splits each incoming sentence into words and emits one tuple per word, anchored so the ack
     * tree extends to the downstream worker.
     */
    private static class SplitterBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split("\\s+")) {
                collector.emit(tuple, new Values(word));
            }
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELD_WORD));
        }
    }

    /**
     * Counts words and parks for a constant floor plus task-index-proportional random noise, so each
     * task's RFC-1889 execute-jitter (the EWMA of {@code |Δ execute-latency|}) differs by construction.
     *
     * <p>Task {@code i} parks for {@code baseDelayUs} µs plus uniform noise in {@code [0, i * baseDelayUs]}
     * µs per tuple. Crucially, the constant floor cancels in the consecutive-difference the jitter
     * estimator takes, so only the noise <i>width</i> drives jitter — not a per-task mean offset (which
     * would cancel and leave every task with identical jitter). Steady-state jitter is {@code width / 3}.
     * For a 4-task setup with the default {@code baseDelayUs = 2000}:
     * <ul>
     *   <li>Task 0: constant ~2 ms — jitter ≈ 0 ms (steadiest)</li>
     *   <li>Task 1: ~2–4 ms — jitter ≈ 0.67 ms</li>
     *   <li>Task 2: ~2–6 ms — jitter ≈ 1.33 ms</li>
     *   <li>Task 3: ~2–8 ms — jitter ≈ 2 ms (jitteriest)</li>
     * </ul>
     * Widths are millisecond-scale so the EWMA jitter gauge (millisecond resolution) records distinct
     * values per task. {@link JitterAwareStreamGrouping} then steers tuples toward the lowest-jitter
     * task (task 0) when feedback is enabled.
     *
     * <p>{@link LockSupport#parkNanos} is used instead of a spin loop so jittery tasks yield the CPU
     * and do not starve the steady task's executor thread.
     */
    private static class JitteryWorkerBolt extends BaseRichBolt {
        private final long baseDelayUs;
        private OutputCollector collector;
        private long baseFloorNs;
        private long jitterWidthNs;
        private final Map<String, Integer> counts = new HashMap<>();

        JitteryWorkerBolt(long baseDelayUs) {
            this.baseDelayUs = baseDelayUs;
        }

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            long baseDelayNs = baseDelayUs * 1_000L;
            // Constant floor parked by every task: keeps each task doing real work, but cancels in the
            // consecutive-difference the RFC-1889 jitter estimator takes, so it adds no jitter.
            this.baseFloorNs = baseDelayNs;
            // Noise WIDTH grows with task index, so execute-jitter (EWMA of |Δlatency|) genuinely differs
            // per task: task 0 is perfectly steady (zero jitter), higher indices are progressively jitterier.
            this.jitterWidthNs = baseDelayNs * context.getThisTaskIndex();
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getString(0);
            counts.merge(word, 1, Integer::sum);
            int count = counts.get(word);

            long noiseNs = jitterWidthNs == 0L ? 0L : (long) (ThreadLocalRandom.current().nextDouble() * jitterWidthNs);
            long sleepNs = baseFloorNs + noiseNs;
            if (sleepNs > 0) {
                LockSupport.parkNanos(sleepNs);
            }

            // Emit anchored so the ack chain continues to SinkBolt, and so execute/process jitter
            // is measured and reported back to SplitterBolt via upstream feedback.
            collector.emit(tuple, new Values(word, count));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELD_WORD, FIELD_COUNT));
        }
    }

    /**
     * Terminal bolt: acks each tuple to complete the tuple tree and drive spout complete-latency.
     */
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
            // terminal — no output
        }
    }
}
