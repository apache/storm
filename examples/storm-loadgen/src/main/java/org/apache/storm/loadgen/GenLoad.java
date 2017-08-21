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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
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
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate a simulated load.
 */
public class GenLoad {
    private static final Logger LOG = LoggerFactory.getLogger(GenLoad.class);
    private static final int TEST_EXECUTE_TIME_DEFAULT = 5;

    /**
     * Main entry point for GenLoad application.
     * @param args the command line args.
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
            .longOpt("parallel")
            .argName("MULTIPLIER")
            .hasArg()
            .desc("How much to scale the topology up or down in parallelism.\n"
                + "The new parallelism will round up to the next whole number\n"
                + "(defaults to 1.0 no scaling)")
            .build());
        options.addOption(Option.builder()
            .longOpt("throughput")
            .argName("MULTIPLIER")
            .hasArg()
            .desc("How much to scale the topology up or down in throughput.\n"
                + "Note this is applied after and build on any parallelism changes.\n"
                + "(defaults to 1.0 no scaling)")
            .build());
        options.addOption(Option.builder()
            .longOpt("local-or-shuffle")
            .desc("replace shuffle grouping with local or shuffle grouping")
            .build());
        options.addOption(Option.builder()
            .longOpt("debug")
            .desc("Print debug information about the adjusted topology before submitting it.")
            .build());
        LoadMetricsServer.addCommandLineOptions(options);
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        Exception commandLineException = null;
        double executeTime = TEST_EXECUTE_TIME_DEFAULT;
        double parallel = 1.0;
        double throughput = 1.0;
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("t")) {
                executeTime = Double.valueOf(cmd.getOptionValue("t"));
            }
            if (cmd.hasOption("parallel")) {
                parallel = Double.parseDouble(cmd.getOptionValue("parallel"));
            }
            if (cmd.hasOption("throughput")) {
                throughput = Double.parseDouble(cmd.getOptionValue("throughput"));
            }
        } catch (ParseException | NumberFormatException e) {
            commandLineException = e;
        }
        if (commandLineException != null || cmd.hasOption('h')) {
            if (commandLineException != null) {
                System.err.println("ERROR " + commandLineException.getMessage());
            }
            new HelpFormatter().printHelp("GenLoad [options] [captured_file]*", options);
            return;
        }
        Config conf = new Config();
        LoadMetricsServer metricServer = new LoadMetricsServer(conf, cmd);

        metricServer.serve();
        String url = metricServer.getUrl();
        int exitStatus = -1;
        try (NimbusClient client = NimbusClient.getConfiguredClient(conf);
             ScopedTopologySet topoNames = new ScopedTopologySet(client.getClient())) {
            for (String topoFile : cmd.getArgList()) {
                try {
                    TopologyLoadConf tlc = readTopology(topoFile);
                    if (parallel != 1.0) {
                        tlc = tlc.scaleParallel(parallel);
                    }
                    if (throughput != 1.0) {
                        tlc = tlc.scaleThroughput(throughput);
                    }
                    if (cmd.hasOption("local-or-shuffle")) {
                        tlc = tlc.replaceShuffleWithLocalOrShuffle();
                    }
                    if (cmd.hasOption("debug")) {
                        LOG.info("DEBUGGING: {}", tlc.toYamlString());
                    }
                    topoNames.add(parseAndSubmit(tlc, url));
                } catch (Exception e) {
                    System.err.println("Could Not Submit Topology From " + topoFile);
                    e.printStackTrace(System.err);
                }
            }

            metricServer.monitorFor(executeTime, client.getClient(), topoNames);
            exitStatus = 0;
        } catch (Exception e) {
            LOG.error("Error trying to run topologies...", e);
        } finally {
            System.exit(exitStatus);
        }
    }

    private static TopologyLoadConf readTopology(String topoFile) throws IOException {
        File f = new File(topoFile);

        TopologyLoadConf tlc = TopologyLoadConf.fromConf(f);
        if (tlc.name == null) {
            String fileName = f.getName();
            int dot = fileName.lastIndexOf('.');
            final String baseName = fileName.substring(0, dot);
            tlc = tlc.withName(baseName);
        }
        return tlc;
    }

    static int uniquifier = 0;

    private static String parseAndSubmit(TopologyLoadConf tlc, String url) throws IOException, InvalidTopologyException,
        AuthorizationException, AlreadyAliveException {

        //First we need some configs
        Config conf = new Config();
        if (tlc.topoConf != null) {
            conf.putAll(tlc.topoConf);
        }
        //For some reason on the new code if ackers is null we get 0???
        Object ackers = conf.get(Config.TOPOLOGY_ACKER_EXECUTORS);
        Object workers = conf.get(Config.TOPOLOGY_WORKERS);
        if (ackers == null || ((Number)ackers).intValue() <= 0) {
            if (workers == null) {
                workers = 1;
            }
            conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, workers);
        }
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);
        conf.registerMetricsConsumer(HttpForwardingMetricsConsumer.class, url, 1);
        Map<String, String> workerMetrics = new HashMap<>();
        if (!NimbusClient.isLocalOverride()) {
            //sigar uses JNI and does not work in local mode
            workerMetrics.put("CPU", "org.apache.storm.metrics.sigar.CPUMetric");
        }
        conf.put(Config.TOPOLOGY_WORKER_METRICS, workerMetrics);
        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 10);

        //Lets build a topology.
        TopologyBuilder builder = new TopologyBuilder();
        for (LoadCompConf spoutConf : tlc.spouts) {
            System.out.println("ADDING SPOUT " + spoutConf.id);
            builder.setSpout(spoutConf.id, new LoadSpout(spoutConf), spoutConf.parallelism);
        }

        Map<String, BoltDeclarer> boltDeclarers = new HashMap<>();
        Map<String, LoadBolt> bolts = new HashMap<>();
        if (tlc.bolts != null) {
            for (LoadCompConf boltConf : tlc.bolts) {
                System.out.println("ADDING BOLT " + boltConf.id);
                LoadBolt lb = new LoadBolt(boltConf);
                bolts.put(boltConf.id, lb);
                boltDeclarers.put(boltConf.id, builder.setBolt(boltConf.id, lb, boltConf.parallelism));
            }
        }

        if (tlc.streams != null) {
            for (InputStream in : tlc.streams) {
                BoltDeclarer declarer = boltDeclarers.get(in.toComponent);
                if (declarer == null) {
                    throw new IllegalArgumentException("to bolt " + in.toComponent + " does not exist");
                }
                LoadBolt lb = bolts.get(in.toComponent);
                lb.add(in);
                in.groupingType.assign(declarer, in);
            }
        }

        String topoName = tlc.name + "-" + uniquifier++;
        StormSubmitter.submitTopology(topoName, conf, builder.createTopology());
        return topoName;
    }
}
