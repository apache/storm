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
import org.apache.storm.grouping.JitterAwareStreamGrouping;
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
 * artificially skewed processing latencies.
 *
 * <p>Pipeline: {@code GenSpout -> SplitterBolt -> JitteryWorkerBolt -> SinkBolt}
 *
 * <p>{@code JitteryWorkerBolt} tasks have task-index-dependent processing delays. Task 0 is fast;
 * each subsequent task is progressively slower. This mimics real-world conditions (GC pressure,
 * I/O, resource contention) where downstream tasks diverge in responsiveness. With upstream
 * feedback enabled, {@link JitterAwareStreamGrouping} routes more tuples to the fastest tasks,
 * improving throughput and reducing complete latency by &ge;10% compared to plain round-robin.
 *
 * <p>Run the baseline and the jitter-aware run back-to-back to compare:
 * <pre>
 *   # Baseline: JitterAwareStreamGrouping falls back to round-robin (no feedback stats).
 *   storm jar storm-perf.jar org.apache.storm.perf.JitterAwareGroupingTopology 120 \
 *       -c topology.upstream.feedback.enable=false
 *
 *   # Jitter-aware: grouping steers tuples to lowest-jitter workers.
 *   storm jar storm-perf.jar org.apache.storm.perf.JitterAwareGroupingTopology 120 \
 *       -c topology.upstream.feedback.enable=true
 * </pre>
 *
 * <p>Tuning knobs (pass with {@code -c key=value}):
 * <ul>
 *   <li>{@code spout.count} — number of spout tasks (default 1)</li>
 *   <li>{@code splitter.count} — number of splitter tasks (default 2)</li>
 *   <li>{@code worker.count} — number of jittery worker tasks (default 4)</li>
 *   <li>{@code sink.count} — number of sink tasks (default 1)</li>
 *   <li>{@code input.file} — path to a text file whose lines are treated as sentences
 *       (e.g. {@code src/main/sampledata/randomwords.txt})</li>
 *   <li>{@code worker.base.delay.us} — per-index processing delay step in µs (default 2000).
 *       Task {@code i} parks for {@code i * base} µs plus up to {@code base} µs of random noise,
 *       so a 4-worker setup has delays of ~0-2, ~2-4, ~4-6, ~6-8 ms. Must be &ge;1000 µs so
 *       the EWMA latency gauge (millisecond resolution) sees distinct values per task.</li>
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
               .customGrouping(SPLITTER_ID, new JitterAwareStreamGrouping());
        builder.setBolt(SINK_ID, new SinkBolt(), sinks)
               .localOrShuffleGrouping(WORKER_ID);

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

        topoConf.put(Config.TOPOLOGY_STATS_EWMA_ENABLE, true);
        topoConf.putIfAbsent(Config.TOPOLOGY_MAX_SPOUT_PENDING, 4000);
        topoConf.putIfAbsent(Config.TOPOLOGY_UPSTREAM_FEEDBACK_FREQ_SECS, 10);
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
     * Counts words and adds a task-index-proportional processing delay to produce deliberately
     * skewed jitter profiles across tasks.
     *
     * <p>Task {@code i} parks for {@code i * baseDelayUs} µs plus up to {@code baseDelayUs} µs of
     * random noise per tuple. For a 4-task setup with the default {@code baseDelayUs = 2000}:
     * <ul>
     *   <li>Task 0: ~0–2 ms (fast)</li>
     *   <li>Task 1: ~2–4 ms</li>
     *   <li>Task 2: ~4–6 ms</li>
     *   <li>Task 3: ~6–8 ms (slow)</li>
     * </ul>
     * Delays are millisecond-scale so the EWMA execute-latency gauge (which stores values in ms)
     * records distinct values per task. {@link JitterAwareStreamGrouping} then steers tuples toward
     * the task with the lowest EWMA execute-latency when feedback is enabled.
     *
     * <p>{@link LockSupport#parkNanos} is used instead of a spin loop so slow tasks yield the CPU
     * and do not starve the fast task's executor thread.
     */
    private static class JitteryWorkerBolt extends BaseRichBolt {
        private final long baseDelayUs;
        private OutputCollector collector;
        private long taskBaselineNs;
        private long baseDelayNs;
        private final Map<String, Integer> counts = new HashMap<>();

        JitteryWorkerBolt(long baseDelayUs) {
            this.baseDelayUs = baseDelayUs;
        }

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            this.baseDelayNs = baseDelayUs * 1_000L;
            // Higher task indices get proportionally larger baseline delays.
            this.taskBaselineNs = baseDelayNs * context.getThisTaskIndex();
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getString(0);
            counts.merge(word, 1, Integer::sum);
            int count = counts.get(word);

            long noiseNs = (long) (ThreadLocalRandom.current().nextDouble() * baseDelayNs);
            long sleepNs = taskBaselineNs + noiseNs;
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
