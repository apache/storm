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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.Config;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.generated.WorkerSummary;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.ObjectReader;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Capture running topologies for load gen later on.
 */
public class CaptureLoad {
    private static final Logger LOG = LoggerFactory.getLogger(CaptureLoad.class);
    public static final String DEFAULT_OUT_DIR = "./loadgen/";

    private static List<Double> extractBoltValues(List<ExecutorSummary> summaries,
                                                  GlobalStreamId id,
                                                  Function<BoltStats, Map<String, Map<GlobalStreamId, Double>>> func) {

        List<Double> ret = new ArrayList<>();
        if (summaries != null) {
            for (ExecutorSummary summ : summaries) {
                if (summ != null && summ.is_set_stats()) {
                    Map<String, Map<GlobalStreamId, Double>> data = func.apply(summ.get_stats().get_specific().get_bolt());
                    if (data != null) {
                        List<Double> subvalues = data.values().stream()
                            .map((subMap) -> subMap.get(id))
                            .filter((value) -> value != null)
                            .collect(Collectors.toList());
                        ret.addAll(subvalues);
                    }
                }
            }
        }
        return ret;
    }

    static TopologyLoadConf captureTopology(Nimbus.Iface client, TopologySummary topologySummary) throws Exception {
        String topologyName = topologySummary.get_name();
        LOG.info("Capturing {}...", topologyName);
        String topologyId = topologySummary.get_id();
        TopologyInfo info = client.getTopologyInfo(topologyId);
        TopologyPageInfo tpinfo = client.getTopologyPageInfo(topologyId, ":all-time", false);
        @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
        StormTopology topo = client.getUserTopology(topologyId);
        //Done capturing topology information...

        Map<String, Object> savedTopoConf = new HashMap<>();
        Map<String, Object> topoConf = (Map<String, Object>) JSONValue.parse(client.getTopologyConf(topologyId));
        for (String key: TopologyLoadConf.IMPORTANT_CONF_KEYS) {
            Object o = topoConf.get(key);
            if (o != null) {
                savedTopoConf.put(key, o);
                LOG.info("with config {}: {}", key, o);
            }
        }
        //Lets use the number of actually scheduled workers as a way to bridge RAS and non-RAS
        int numWorkers = tpinfo.get_num_workers();
        if (savedTopoConf.containsKey(Config.TOPOLOGY_WORKERS)) {
            numWorkers = Math.max(numWorkers, ((Number) savedTopoConf.get(Config.TOPOLOGY_WORKERS)).intValue());
        }
        savedTopoConf.put(Config.TOPOLOGY_WORKERS, numWorkers);

        Map<String, LoadCompConf.Builder> boltBuilders = new HashMap<>();
        Map<String, LoadCompConf.Builder> spoutBuilders = new HashMap<>();
        List<InputStream.Builder> inputStreams = new ArrayList<>();
        Map<GlobalStreamId, OutputStream.Builder> outStreams = new HashMap<>();

        //Bolts
        if (topo.get_bolts() != null) {
            for (Map.Entry<String, Bolt> boltSpec : topo.get_bolts().entrySet()) {
                String boltComp = boltSpec.getKey();
                LOG.info("Found bolt {}...", boltComp);
                Bolt bolt = boltSpec.getValue();
                ComponentCommon common = bolt.get_common();
                Map<GlobalStreamId, Grouping> inputs = common.get_inputs();
                if (inputs != null) {
                    for (Map.Entry<GlobalStreamId, Grouping> input : inputs.entrySet()) {
                        GlobalStreamId id = input.getKey();
                        LOG.info("with input {}...", id);
                        Grouping grouping = input.getValue();
                        InputStream.Builder builder = new InputStream.Builder()
                            .withId(id.get_streamId())
                            .withFromComponent(id.get_componentId())
                            .withToComponent(boltComp)
                            .withGroupingType(grouping);
                        inputStreams.add(builder);
                    }
                }
                Map<String, StreamInfo> outputs = common.get_streams();
                if (outputs != null) {
                    for (String name : outputs.keySet()) {
                        GlobalStreamId id = new GlobalStreamId(boltComp, name);
                        LOG.info("and output {}...", id);
                        OutputStream.Builder builder = new OutputStream.Builder()
                            .withId(name);
                        outStreams.put(id, builder);
                    }
                }
                LoadCompConf.Builder builder = new LoadCompConf.Builder()
                    .withParallelism(common.get_parallelism_hint())
                    .withId(boltComp);
                boltBuilders.put(boltComp, builder);
            }

            Map<String, Map<String, Double>> boltResources = getBoltsResources(topo, topoConf);
            for (Map.Entry<String, Map<String, Double>> entry: boltResources.entrySet()) {
                LoadCompConf.Builder bd = boltBuilders.get(entry.getKey());
                if (bd != null) {
                    Map<String, Double> resources = entry.getValue();
                    Double cpu = resources.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);
                    if (cpu != null) {
                        bd.withCpuLoad(cpu);
                    }
                    Double mem = resources.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
                    if (mem != null) {
                        bd.withMemoryLoad(mem);
                    }
                }
            }
        }

        //Spouts
        if (topo.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spoutSpec : topo.get_spouts().entrySet()) {
                String spoutComp = spoutSpec.getKey();
                LOG.info("Found Spout {}...", spoutComp);
                SpoutSpec spout = spoutSpec.getValue();
                ComponentCommon common = spout.get_common();

                Map<String, StreamInfo> outputs = common.get_streams();
                if (outputs != null) {
                    for (String name : outputs.keySet()) {
                        GlobalStreamId id = new GlobalStreamId(spoutComp, name);
                        LOG.info("with output {}...", id);
                        OutputStream.Builder builder = new OutputStream.Builder()
                            .withId(name);
                        outStreams.put(id, builder);
                    }
                }
                LoadCompConf.Builder builder = new LoadCompConf.Builder()
                    .withParallelism(common.get_parallelism_hint())
                    .withId(spoutComp);
                spoutBuilders.put(spoutComp, builder);
            }

            Map<String, Map<String, Double>> spoutResources = getSpoutsResources(topo, topoConf);
            for (Map.Entry<String, Map<String, Double>> entry: spoutResources.entrySet()) {
                LoadCompConf.Builder sd = spoutBuilders.get(entry.getKey());
                if (sd != null) {
                    Map<String, Double> resources = entry.getValue();
                    Double cpu = resources.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);
                    if (cpu != null) {
                        sd.withCpuLoad(cpu);
                    }
                    Double mem = resources.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
                    if (mem != null) {
                        sd.withMemoryLoad(mem);
                    }
                }
            }
        }

        //Stats...
        Map<String, List<ExecutorSummary>> byComponent = new HashMap<>();
        for (ExecutorSummary executor: info.get_executors()) {
            String component = executor.get_component_id();
            List<ExecutorSummary> list = byComponent.get(component);
            if (list == null) {
                list = new ArrayList<>();
                byComponent.put(component, list);
            }
            list.add(executor);
        }

        List<InputStream> streams = new ArrayList<>(inputStreams.size());
        //Compute the stats for the different input streams
        for (InputStream.Builder builder : inputStreams) {
            GlobalStreamId streamId = new GlobalStreamId(builder.getFromComponent(), builder.getId());
            List<ExecutorSummary> summaries = byComponent.get(builder.getToComponent());
            //Execute and process latency...
            builder.withProcessTime(new NormalDistStats(
                extractBoltValues(summaries, streamId, BoltStats::get_process_ms_avg)));
            builder.withExecTime(new NormalDistStats(
                extractBoltValues(summaries, streamId, BoltStats::get_execute_ms_avg)));
            //InputStream is done
            streams.add(builder.build());
        }

        //There is a bug in some versions that returns 0 for the uptime.
        // To work around it we should get it an alternative (working) way.
        Map<String, Integer> workerToUptime = new HashMap<>();
        for (WorkerSummary ws : tpinfo.get_workers()) {
            workerToUptime.put(ws.get_supervisor_id() + ":" + ws.get_port(), ws.get_uptime_secs());
        }
        LOG.debug("WORKER TO UPTIME {}", workerToUptime);

        for (Map.Entry<GlobalStreamId, OutputStream.Builder> entry : outStreams.entrySet()) {
            OutputStream.Builder builder = entry.getValue();
            GlobalStreamId id = entry.getKey();
            List<Double> emittedRate = new ArrayList<>();
            List<ExecutorSummary> summaries = byComponent.get(id.get_componentId());
            if (summaries != null) {
                for (ExecutorSummary summary: summaries) {
                    if (summary.is_set_stats()) {
                        int uptime = summary.get_uptime_secs();
                        LOG.debug("UPTIME {}", uptime);
                        if (uptime <= 0) {
                            //Likely it is because of a bug, so try to get it another way
                            String key = summary.get_host() + ":" + summary.get_port();
                            uptime = workerToUptime.getOrDefault(key, 1);
                            LOG.debug("Getting uptime for worker {}, {}", key, uptime);
                        }
                        for (Map.Entry<String, Map<String, Long>> statEntry : summary.get_stats().get_emitted().entrySet()) {
                            String timeWindow = statEntry.getKey();
                            long timeSecs = uptime;
                            try {
                                timeSecs = Long.valueOf(timeWindow);
                            } catch (NumberFormatException e) {
                                //Ignored...
                            }
                            timeSecs = Math.min(timeSecs, uptime);
                            Long count = statEntry.getValue().get(id.get_streamId());
                            if (count != null) {
                                LOG.debug("{} emitted {} for {} secs or {} tuples/sec",
                                    id, count, timeSecs, count.doubleValue() / timeSecs);
                                emittedRate.add(count.doubleValue() / timeSecs);
                            }
                        }
                    }
                }
            }
            builder.withRate(new NormalDistStats(emittedRate));

            //The OutputStream is done
            LoadCompConf.Builder comp = boltBuilders.get(id.get_componentId());
            if (comp == null) {
                comp = spoutBuilders.get(id.get_componentId());
            }
            comp.withStream(builder.build());
        }

        List<LoadCompConf> spouts = spoutBuilders.values().stream()
            .map((b) -> b.build())
            .collect(Collectors.toList());

        List<LoadCompConf> bolts = boltBuilders.values().stream()
            .map((b) -> b.build())
            .collect(Collectors.toList());

        return new TopologyLoadConf(topologyName, savedTopoConf, spouts, bolts, streams);
    }

    /**
     * Main entry point for CaptureLoad command.
     * @param args the arguments to the command
     * @throws Exception on any error
     */
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(Option.builder("a")
            .longOpt("anonymize")
            .desc("Strip out any possibly identifiable information")
            .build());
        options.addOption(Option.builder("o")
            .longOpt("output-dir")
            .argName("<file>")
            .hasArg()
            .desc("Where to write (defaults to " + DEFAULT_OUT_DIR + ")")
            .build());
        options.addOption(Option.builder("h")
            .longOpt("help")
            .desc("Print a help message")
            .build());
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        boolean printHelp = false;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("ERROR " + e.getMessage());
            printHelp = true;
        }
        if (printHelp || cmd.hasOption('h')) {
            new HelpFormatter().printHelp("CaptureLoad [options] [topologyName]*", options);
            return;
        }

        Config conf = new Config();
        int exitStatus = -1;
        String outputDir = DEFAULT_OUT_DIR;
        if (cmd.hasOption('o')) {
            outputDir = cmd.getOptionValue('o');
        }
        File baseOut = new File(outputDir);
        LOG.info("Will save captured topologies to {}", baseOut);
        baseOut.mkdirs();

        try (NimbusClient nc = NimbusClient.getConfiguredClient(conf)) {
            Nimbus.Iface client = nc.getClient();
            List<String> topologyNames = cmd.getArgList();

            for (TopologySummary topologySummary: client.getTopologySummaries()) {
                if (topologyNames.isEmpty() || topologyNames.contains(topologySummary.get_name())) {
                    TopologyLoadConf capturedConf = captureTopology(client, topologySummary);
                    if (cmd.hasOption('a')) {
                        capturedConf = capturedConf.anonymize();
                    }
                    capturedConf.writeTo(new File(baseOut, capturedConf.name + ".yaml"));
                }
            }

            exitStatus = 0;
        } catch (Exception e) {
            LOG.error("Error trying to capture topologies...", e);
        } finally {
            System.exit(exitStatus);
        }
    }

    //ResourceUtils.java is not a available on the classpath to let us parse out the resources we want.
    // So we have copied and pasted some of the needed methods here. (with a few changes to logging)
    static Map<String, Map<String, Double>> getBoltsResources(StormTopology topology, Map<String, Object> topologyConf) {
        Map<String, Map<String, Double>> boltResources = new HashMap<>();
        if (topology.get_bolts() != null) {
            for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
                Map<String, Double> topologyResources = parseResources(bolt.getValue().get_common().get_json_conf());
                checkInitialization(topologyResources, bolt.getValue().toString(), topologyConf);
                boltResources.put(bolt.getKey(), topologyResources);
            }
        }
        return boltResources;
    }

    static Map<String, Map<String, Double>> getSpoutsResources(StormTopology topology, Map<String, Object> topologyConf) {
        Map<String, Map<String, Double>> spoutResources = new HashMap<>();
        if (topology.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spout : topology.get_spouts().entrySet()) {
                Map<String, Double> topologyResources = parseResources(spout.getValue().get_common().get_json_conf());
                checkInitialization(topologyResources, spout.getValue().toString(), topologyConf);
                spoutResources.put(spout.getKey(), topologyResources);
            }
        }
        return spoutResources;
    }

    static Map<String, Double> parseResources(String input) {
        Map<String, Double> topologyResources = new HashMap<>();
        JSONParser parser = new JSONParser();
        LOG.debug("Input to parseResources {}", input);
        try {
            if (input != null) {
                Object obj = parser.parse(input);
                JSONObject jsonObject = (JSONObject) obj;
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)) {
                    Double topoMemOnHeap = ObjectReader
                        .getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null);
                    topologyResources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, topoMemOnHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
                    Double topoMemOffHeap = ObjectReader
                        .getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null);
                    topologyResources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, topoMemOffHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
                    Double topoCpu = ObjectReader.getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT),
                        null);
                    topologyResources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, topoCpu);
                }
                LOG.debug("Topology Resources {}", topologyResources);
            }
        } catch (org.json.simple.parser.ParseException e) {
            LOG.error("Failed to parse component resources is:" + e.toString(), e);
            return null;
        }
        return topologyResources;
    }

    /**
     * Checks if the topology's resource requirements are initialized.
     * Will modify topologyResources by adding the appropriate defaults
     * @param topologyResources map of resouces requirements
     * @param componentId component for which initialization is being conducted
     * @param topologyConf topology configuration
     * @throws Exception on any error
     */
    public static void checkInitialization(Map<String, Double> topologyResources, String componentId, Map<String, Object> topologyConf) {
        StringBuilder msgBuilder = new StringBuilder();

        for (String resourceName : topologyResources.keySet()) {
            msgBuilder.append(checkInitResource(topologyResources, topologyConf, resourceName));
        }

        if (msgBuilder.length() > 0) {
            String resourceDefaults = msgBuilder.toString();
            LOG.debug(
                    "Unable to extract resource requirement for Component {} \n Resources : {}",
                    componentId, resourceDefaults);
        }
    }

    private static String checkInitResource(Map<String, Double> topologyResources, Map<String, Object> topologyConf, String resourceName) {
        StringBuilder msgBuilder = new StringBuilder();
        if (topologyResources.containsKey(resourceName)) {
            Double resourceValue = (Double) topologyConf.getOrDefault(resourceName, null);
            if (resourceValue != null) {
                topologyResources.put(resourceName, resourceValue);
                msgBuilder.append(resourceName.substring(resourceName.lastIndexOf(".")) + " has been set to " + resourceValue);
            }
        }

        return msgBuilder.toString();
    }

}
