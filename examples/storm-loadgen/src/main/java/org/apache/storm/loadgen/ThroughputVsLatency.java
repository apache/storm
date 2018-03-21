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

package org.apache.storm.loadgen;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WordCount but the spout goes at a predefined rate and we collect
 * proper latency statistics.
 */
public class ThroughputVsLatency {
    private static final Logger LOG = LoggerFactory.getLogger(ThroughputVsLatency.class);
    private static final int TEST_EXECUTE_TIME_DEFAULT = 5;
    private static final long DEFAULT_RATE_PER_SECOND = 500;
    private static final String DEFAULT_TOPO_NAME = "wc-test";
    private static final int DEFAULT_NUM_SPOUTS = 1;
    private static final int DEFAULT_NUM_SPLITS = 1;
    private static final int DEFAULT_NUM_COUNTS = 1;

    public static class FastRandomSentenceSpout extends LoadSpout {
        static final String[] SENTENCES = new String[] {
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"
        };

        /**
         * Constructor.
         * @param ratePerSecond the rate to emite tuples at.
         */
        public FastRandomSentenceSpout(long ratePerSecond) {
            super(ratePerSecond);
        }

        @Override
        protected Values getNextValues(OutputStreamEngine se) {
            String sentence = SENTENCES[se.nextInt(SENTENCES.length)];
            return new Values(sentence);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }

    public static class SplitSentence extends BaseBasicBolt {
        private ExecAndProcessLatencyEngine sleep;
        private int executorIndex;

        public SplitSentence(SlowExecutorPattern slowness) {
            super();
            sleep = new ExecAndProcessLatencyEngine(slowness);
        }

        @Override
        public void prepare(Map<String, Object> stormConf,
                TopologyContext context) {
            executorIndex = context.getThisTaskIndex();
            sleep.prepare();
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            sleep.simulateProcessAndExecTime(executorIndex, Time.nanoTime(), null , () -> {
                String sentence = tuple.getString(0);
                for (String word: sentence.split("\\s+")) {
                    collector.emit(new Values(word, 1));
                }
            });
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    /**
     * The main entry point for ThroughputVsLatency.
     * @param args the command line args
     * @throws Exception on any error.
     */
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(Option.builder("h")
            .longOpt("help")
            .desc("Print a help message")
            .build());
        options.addOption(Option.builder("t")
            .longOpt("test-time")
            .argName("MINS")
            .hasArg()
            .desc("How long to run the tests for in mins (defaults to " + TEST_EXECUTE_TIME_DEFAULT + ")")
            .build());
        options.addOption(Option.builder()
            .longOpt("rate")
            .argName("SENTENCES/SEC")
            .hasArg()
            .desc("How many sentences per second to run. (defaults to " + DEFAULT_RATE_PER_SECOND + ")")
            .build());
        options.addOption(Option.builder()
            .longOpt("name")
            .argName("TOPO_NAME")
            .hasArg()
            .desc("Name of the topology to run (defaults to " + DEFAULT_TOPO_NAME + ")")
            .build());
        options.addOption(Option.builder()
            .longOpt("spouts")
            .argName("NUM")
            .hasArg()
            .desc("Number of spouts to use (defaults to " + DEFAULT_NUM_SPOUTS + ")")
            .build());
        options.addOption(Option.builder()
            .longOpt("splitters")
            .argName("NUM")
            .hasArg()
            .desc("Number of splitter bolts to use (defaults to " + DEFAULT_NUM_SPLITS + ")")
            .build());
        options.addOption(Option.builder()
            .longOpt("splitter-imbalance")
            .argName("MS(:COUNT)?")
            .hasArg()
            .desc("The number of ms that the first COUNT splitters will wait before processing.  This creates an imbalance "
                + "that helps test load aware groupings (defaults to 0:1)")
            .build());
        options.addOption(Option.builder()
            .longOpt("counters")
            .argName("NUM")
            .hasArg()
            .desc("Number of counter bolts to use (defaults to " + DEFAULT_NUM_COUNTS + ")")
            .build());
        LoadMetricsServer.addCommandLineOptions(options);
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        Exception commandLineException = null;
        SlowExecutorPattern slowness = null;
        double numMins = TEST_EXECUTE_TIME_DEFAULT;
        double ratePerSecond = DEFAULT_RATE_PER_SECOND;
        String name = DEFAULT_TOPO_NAME;
        int numSpouts = DEFAULT_NUM_SPOUTS;
        int numSplits = DEFAULT_NUM_SPLITS;
        int numCounts = DEFAULT_NUM_COUNTS;
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("t")) {
                numMins = Double.valueOf(cmd.getOptionValue("t"));
            }
            if (cmd.hasOption("rate")) {
                ratePerSecond = Double.parseDouble(cmd.getOptionValue("rate"));
            }
            if (cmd.hasOption("name")) {
                name = cmd.getOptionValue("name");
            }
            if (cmd.hasOption("spouts")) {
                numSpouts = Integer.parseInt(cmd.getOptionValue("spouts"));
            }
            if (cmd.hasOption("splitters")) {
                numSplits = Integer.parseInt(cmd.getOptionValue("splitters"));
            }
            if (cmd.hasOption("counters")) {
                numCounts = Integer.parseInt(cmd.getOptionValue("counters"));
            }
            if (cmd.hasOption("splitter-imbalance")) {
                slowness = SlowExecutorPattern.fromString(cmd.getOptionValue("splitter-imbalance"));
            }
        } catch (ParseException | NumberFormatException e) {
            commandLineException = e;
        }
        if (commandLineException != null || cmd.hasOption('h')) {
            if (commandLineException != null) {
                System.err.println("ERROR " + commandLineException.getMessage());
            }
            new HelpFormatter().printHelp("ThroughputVsLatency [options]", options);
            return;
        }

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("target_rate", ratePerSecond);
        metrics.put("spout_parallel", numSpouts);
        metrics.put("split_parallel", numSplits);
        metrics.put("count_parallel", numCounts);

        Config conf = new Config();
        Map<String, Object> sysConf = Utils.readStormConfig();
        LoadMetricsServer metricServer = new LoadMetricsServer(sysConf, cmd, metrics);
        metricServer.serve();
        String url = metricServer.getUrl();

        NimbusClient client = NimbusClient.getConfiguredClient(sysConf);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);
        conf.registerMetricsConsumer(HttpForwardingMetricsConsumer.class, url, 1);
        Map<String, String> workerMetrics = new HashMap<>();
        if (!NimbusClient.isLocalOverride()) {
            //sigar uses JNI and does not work in local mode
            workerMetrics.put("CPU", "org.apache.storm.metrics.sigar.CPUMetric");
        }
        conf.put(Config.TOPOLOGY_WORKER_METRICS, workerMetrics);
        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 10);
        conf.put(Config.TOPOLOGY_WORKER_GC_CHILDOPTS,
            "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC "
                + "-XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled");
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx2g");

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new FastRandomSentenceSpout((long) ratePerSecond / numSpouts), numSpouts);
        builder.setBolt("split", new SplitSentence(slowness), numSplits)
            .shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), numCounts).fieldsGrouping("split", new Fields("word"));

        int exitStatus = -1;
        try (ScopedTopologySet topologyNames = new ScopedTopologySet(client.getClient())) {
            StormSubmitter.submitTopology(name, conf, builder.createTopology());
            topologyNames.add(name);

            metricServer.monitorFor(numMins, client.getClient(), topologyNames);
            exitStatus = 0;
        } catch (Exception e) {
            LOG.error("Error while running test", e);
        } finally {
            System.exit(exitStatus);
        }
    }
}
