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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
import org.apache.storm.topology.SpoutDeclarer;
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
    private static final Pattern MULTI_PATTERN = Pattern.compile(
        "(?<value>[^:?]+)(?::(?<topo>[^:]*):(?<comp>.*))?");

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
            .argName("MULTIPLIER(:TOPO:COMP)?")
            .hasArg()
            .desc("How much to scale the topology up or down in parallelism. "
                + "The new parallelism will round up to the next whole number. "
                + "If a topology + component is supplied only that component will be scaled. "
                + "If topo or component is blank or a '*' all topologies or components matched will be scaled. "
                + "Only 1 scaling rule, the most specific, will be applied to a component. Providing a topology name is considered more "
                + "specific than not providing one."
                + "(defaults to 1.0 no scaling)")
            .build());
        options.addOption(Option.builder()
            .longOpt("throughput")
            .argName("MULTIPLIER(:TOPO:COMP)?")
            .hasArg()
            .desc("How much to scale the topology up or down in throughput. "
                + "If a topology + component is supplied only that component will be scaled. "
                + "If topo or component is blank or a '*' all topologies or components matched will be scaled. "
                + "Only 1 scaling rule, the most specific, will be applied to a component. Providing a topology name is considered more "
                + "specific than not providing one."
                + "(defaults to 1.0 no scaling)")
            .build());
        options.addOption(Option.builder()
            .longOpt("local-or-shuffle")
            .desc("replace shuffle grouping with local or shuffle grouping")
            .build());
        options.addOption(Option.builder()
            .longOpt("imbalance")
            .argName("MS(:COUNT)?:TOPO:COMP")
            .hasArg()
            .desc("The number of ms that the first COUNT of TOPO:COMP will wait before processing.  This creates an imbalance "
                + "that helps test load aware groupings. By default there is no imbalance.  If no count is given it defaults to 1")
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
        double globalParallel = 1.0;
        Map<String, Double> topoSpecificParallel = new HashMap<>();
        double globalThroughput = 1.0;
        Map<String, Double> topoSpecificThroughput = new HashMap<>();
        Map<String, SlowExecutorPattern> topoSpecificImbalance = new HashMap<>();
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("t")) {
                executeTime = Double.valueOf(cmd.getOptionValue("t"));
            }
            if (cmd.hasOption("parallel")) {
                for (String stringParallel : cmd.getOptionValues("parallel")) {
                    Matcher m = MULTI_PATTERN.matcher(stringParallel);
                    if (!m.matches()) {
                        throw new ParseException("--parallel " + stringParallel + " is not in the format MULTIPLIER(:TOPO:COMP)?");
                    }
                    double parallel = Double.parseDouble(m.group("value"));
                    String topo = m.group("topo");
                    if (topo == null || topo.isEmpty()) {
                        topo = "*";
                    }
                    String comp = m.group("comp");
                    if (comp == null || comp.isEmpty()) {
                        comp = "*";
                    }
                    if ("*".equals(topo) && "*".equals(comp)) {
                        globalParallel = parallel;
                    } else {
                        topoSpecificParallel.put(topo + ":" + comp, parallel);
                    }
                }
            }
            if (cmd.hasOption("throughput")) {
                for (String stringThroughput : cmd.getOptionValues("throughput")) {
                    Matcher m = MULTI_PATTERN.matcher(stringThroughput);
                    if (!m.matches()) {
                        throw new ParseException("--throughput " + stringThroughput + " is not in the format MULTIPLIER(:TOPO:COMP)?");
                    }
                    double throughput = Double.parseDouble(m.group("value"));
                    String topo = m.group("topo");
                    if (topo == null || topo.isEmpty()) {
                        topo = "*";
                    }
                    String comp = m.group("comp");
                    if (comp == null || comp.isEmpty()) {
                        comp = "*";
                    }
                    if ("*".equals(topo) && "*".equals(comp)) {
                        globalThroughput = throughput;
                    } else {
                        topoSpecificThroughput.put(topo + ":" + comp, throughput);
                    }
                }
            }
            if (cmd.hasOption("imbalance")) {
                for (String stringImbalance : cmd.getOptionValues("imbalance")) {
                    //We require there to be both a topology and a component in this case, so parse it out as such.
                    String [] parts = stringImbalance.split(":");
                    if (parts.length < 3 || parts.length > 4) {
                        throw new ParseException(stringImbalance + " does not appear to match the expected pattern");
                    } else if (parts.length == 3) {
                        topoSpecificImbalance.put(parts[1] + ":" + parts[2], SlowExecutorPattern.fromString(parts[0]));
                    } else { //== 4
                        topoSpecificImbalance.put(parts[2] + ":" + parts[3],
                            SlowExecutorPattern.fromString(parts[0] + ":" + parts[1]));
                    }
                }
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
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("parallel_adjust", globalParallel);
        metrics.put("throughput_adjust", globalThroughput);
        metrics.put("local_or_shuffle", cmd.hasOption("local-or-shuffle"));
        metrics.put("topo_parallel", topoSpecificParallel.entrySet().stream().map((entry) -> entry.getValue() + ":" + entry.getKey())
            .collect(Collectors.toList()));
        metrics.put("topo_throuhgput", topoSpecificThroughput.entrySet().stream().map((entry) -> entry.getValue() + ":" + entry.getKey())
            .collect(Collectors.toList()));
        metrics.put("slow_execs", topoSpecificImbalance.entrySet().stream().map((entry) -> entry.getValue() + ":" + entry.getKey())
            .collect(Collectors.toList()));

        Config conf = new Config();
        LoadMetricsServer metricServer = new LoadMetricsServer(conf, cmd, metrics);

        metricServer.serve();
        String url = metricServer.getUrl();
        int exitStatus = -1;
        try (NimbusClient client = NimbusClient.getConfiguredClient(conf);
             ScopedTopologySet topoNames = new ScopedTopologySet(client.getClient())) {
            for (String topoFile : cmd.getArgList()) {
                try {
                    TopologyLoadConf tlc = readTopology(topoFile);
                    tlc = tlc.scaleParallel(globalParallel, topoSpecificParallel);
                    tlc = tlc.scaleThroughput(globalThroughput, topoSpecificThroughput);
                    tlc = tlc.overrideSlowExecs(topoSpecificImbalance);
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

    private static int uniquifier = 0;

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
        if (ackers == null || ((Number) ackers).intValue() <= 0) {
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
            SpoutDeclarer sd = builder.setSpout(spoutConf.id, new LoadSpout(spoutConf), spoutConf.parallelism);
            if (spoutConf.memoryLoad > 0) {
                sd.setMemoryLoad(spoutConf.memoryLoad);
            }
            if (spoutConf.cpuLoad > 0) {
                sd.setCPULoad(spoutConf.cpuLoad);
            }
        }

        Map<String, BoltDeclarer> boltDeclarers = new HashMap<>();
        Map<String, LoadBolt> bolts = new HashMap<>();
        if (tlc.bolts != null) {
            for (LoadCompConf boltConf : tlc.bolts) {
                System.out.println("ADDING BOLT " + boltConf.id);
                LoadBolt lb = new LoadBolt(boltConf);
                bolts.put(boltConf.id, lb);
                BoltDeclarer bd = builder.setBolt(boltConf.id, lb, boltConf.parallelism);
                if (boltConf.memoryLoad > 0) {
                    bd.setMemoryLoad(boltConf.memoryLoad);
                }
                if (boltConf.cpuLoad > 0) {
                    bd.setCPULoad(boltConf.cpuLoad);
                }
                boltDeclarers.put(boltConf.id, bd);
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
