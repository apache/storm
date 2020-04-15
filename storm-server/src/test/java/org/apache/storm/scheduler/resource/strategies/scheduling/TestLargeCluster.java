/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource.strategies.scheduling;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.scheduler.resource.normalization.NormalizedResources;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourcesExtension;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@ExtendWith({NormalizedResourcesExtension.class})
public class TestLargeCluster {
    private static final Logger LOG = LoggerFactory.getLogger(TestLargeCluster.class);

    public static final String TEST_CLUSTER_NAME = "largeCluster01";
    public static final String TEST_RESOURCE_PATH = "clusterconf/" + TEST_CLUSTER_NAME;
    public static final String COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING = "code.ser";
    public static final String COMPRESSED_SERIALIZED_CONFIG_FILENAME_ENDING = "conf.ser";

    private static IScheduler scheduler = null;

    @AfterEach
    public void cleanup() {
        if (scheduler != null) {
            scheduler.cleanup();
            scheduler = null;
        }
    }

    /**
     * Get the list of serialized topology (*code.ser) and configuration (*conf.ser)
     * resource files in the path. The resources are sorted so that paired topology and conf
     * files are sequential. Unpaired files may be ignored by the caller.
     *
     * @param path directory in which resources exist.
     * @return
     * @throws IOException
     */
    public static List<String> getResourceFiles(String path) throws IOException {
        List<String> fileNames = new ArrayList<>();

        try (
                InputStream in = getResourceAsStream(path);
                BufferedReader br = new BufferedReader(new InputStreamReader(in))
        ) {
            String resource;

            while ((resource = br.readLine()) != null) {
                if (resource.endsWith(COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING)
                        || resource.endsWith(COMPRESSED_SERIALIZED_CONFIG_FILENAME_ENDING)) {
                    fileNames.add(path + "/" + resource);
                }
            }
            Collections.sort(fileNames);
        }
        return fileNames;
    }

    /**
     * InputStream to read the fully qualified resource path.
     *
     * @param resource
     * @return
     */
    public static InputStream getResourceAsStream(String resource) {
        final InputStream in = getContextClassLoader().getResourceAsStream(resource);
        return in == null ? ClassLoader.getSystemClassLoader().getResourceAsStream(resource) : in;
    }

    /**
     * Read the contents of the fully qualified resource path.
     *
     * @param resource
     * @return
     * @throws Exception
     */
    public static byte[] getResourceAsBytes(String resource) throws Exception {
        InputStream in = getResourceAsStream(resource);
        if (in == null) {
            return null;
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            while (in.available() > 0) {
                out.write(in.read());
            }
            return out.toByteArray();
        }
    }

    public static ClassLoader getContextClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    /**
     * Create an array of TopologyDetails by reading serialized files for topology and configuration in the
     * resource path.
     *
     * @param failOnParseError throw exception if there are unmatched files, otherwise ignore unmatched and read errors.
     * @return An array of TopologyDetails representing resource files.
     * @throws Exception
     */
    public static TopologyDetails[] createTopoDetailsArray(boolean failOnParseError) throws Exception {
        List<TopologyDetails> topoDetailsList = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        List<String> resources = getResourceFiles(TEST_RESOURCE_PATH);
        Map<String, String> codeResourceMap = new TreeMap<>();
        Map<String, String> confResourceMap = new HashMap<>();
        for (int i = 0 ; i < resources.size() ; i++) {
            String resource = resources.get(i);
            int idxOfSlash = resource.lastIndexOf("/");
            int idxOfDash = resource.lastIndexOf("-");
            String nm = idxOfDash > idxOfSlash
                    ? resource.substring(idxOfSlash + 1, idxOfDash)
                    : resource.substring(idxOfSlash + 1, resource.length() - COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING.length());
            if (resource.endsWith(COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING)) {
                codeResourceMap.put(nm, resource);
            } else if (resource.endsWith(COMPRESSED_SERIALIZED_CONFIG_FILENAME_ENDING)) {
                confResourceMap.put(nm, resource);
            } else {
                LOG.info("Ignoring unsupported resource file " + resource);
            }
        }
        String[] examinedConfParams = {
                Config.TOPOLOGY_NAME,
                Config.TOPOLOGY_SCHEDULER_STRATEGY,
                Config.TOPOLOGY_PRIORITY,
                Config.TOPOLOGY_WORKERS,
                Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB,
                Config.TOPOLOGY_SUBMITTER_USER,
                Config.TOPOLOGY_ACKER_CPU_PCORE_PERCENT,
                Config.TOPOLOGY_ACKER_RESOURCES_OFFHEAP_MEMORY_MB,
                Config.TOPOLOGY_ACKER_RESOURCES_ONHEAP_MEMORY_MB,
        };

        for (String nm : codeResourceMap.keySet()) {
            String codeResource = codeResourceMap.get(nm);
            if (!confResourceMap.containsKey(nm)) {
                String err = String.format("Ignoring topology file %s because of missing config file for %s", codeResource, nm);
                errors.add(err);
                LOG.error(err);
                continue;
            }
            String confResource = confResourceMap.get(nm);
            LOG.info("Found matching topology and config files: {}, {}", codeResource, confResource);
            StormTopology stormTopology;
            try {
                stormTopology = Utils.deserialize(getResourceAsBytes(codeResource), StormTopology.class);
            } catch (Exception ex) {
                String err = String.format("Cannot read topology from resource %s", codeResource);
                errors.add(err);
                LOG.error(err, ex);
                continue;
            }

            Map<String, Object> conf;
            try {
                conf = Utils.fromCompressedJsonConf(getResourceAsBytes(confResource));
            } catch (RuntimeException | IOException ex) {
                String err = String.format("Cannot read configuration from resource %s", confResource);
                errors.add(err);
                LOG.error(err, ex);
                continue;
            }
            // fix 0.10 conf class names
            String[] configParamsToFix = {Config.TOPOLOGY_SCHEDULER_STRATEGY, Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN,
                    DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY };
            for (String configParam: configParamsToFix) {
                if (!conf.containsKey(configParam)) {
                    continue;
                }
                String className = (String) conf.get(configParam);
                if (className.startsWith("backtype")) {
                    className = className.replace("backtype", "org.apache");
                    conf.put(configParam, className);
                }
            }
            // fix conf params used by ConstraintSolverStrategy
            if (!conf.containsKey(DaemonConfig.RESOURCE_AWARE_SCHEDULER_MAX_STATE_SEARCH)) {
                conf.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_MAX_STATE_SEARCH, 10_000);
            }
            if (!conf.containsKey(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH)) {
                conf.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, 10_000);
            }

            String topoId = nm;
            String topoName = (String) conf.getOrDefault(Config.TOPOLOGY_NAME, nm);

            // conf
            StringBuffer sb = new StringBuffer("Config for " + nm + ": ");
            for (String param : examinedConfParams) {
                Object val = conf.getOrDefault(param, "<null>");
                sb.append(param).append("=").append(val).append(", ");
            }
            LOG.info(sb.toString());

            // topo
            Map<ExecutorDetails, String> execToComp = TestUtilsForResourceAwareScheduler.genExecsAndComps(stormTopology);
            LOG.info("Topology \"{}\" spouts={}, bolts={}, execToComp size is {}", topoName,
                    stormTopology.get_spouts_size(), stormTopology.get_bolts_size(), execToComp.size());
            int numWorkers = Integer.parseInt("" + conf.getOrDefault(Config.TOPOLOGY_WORKERS, "0"));
            TopologyDetails topo = new TopologyDetails(topoId, conf, stormTopology,  numWorkers,
                    execToComp, Time.currentTimeSecs(), "user");
            topo.getComponents(); // sanity check - normally this should not fail

            topoDetailsList.add(topo);
        }
        if (!errors.isEmpty() && failOnParseError) {
            throw new Exception("Unable to parse all serialized objects\n\t" + String.join("\n\t", errors));
        }
        return topoDetailsList.toArray(new TopologyDetails[0]);
    }

    /**
     * Check if the files in the resource directory are matched, can be read properly, and code/config files occur
     * in matched pairs.
     *
     * @throws Exception showing bad and unmatched resource files.
     */
    @Test
    public void testReadSerializedTopologiesAndConfigs() throws Exception {
        List<String> resources = getResourceFiles(TEST_RESOURCE_PATH);
        Assert.assertTrue("No resource files found in " + TEST_RESOURCE_PATH, !resources.isEmpty());
        TopologyDetails[] topoDetailsArray = createTopoDetailsArray(true);
    }

    /**
     * Create one supervisor and add to the supervisors list.
     *
     * @param rack rack-number
     * @param superInRack supervisor number in the rack
     * @param cpu percentage
     * @param mem in megabytes
     * @param numPorts number of ports on this supervisor
     * @param sups returned map os supervisors
     */
    private static void createAndAddOneSupervisor(
            int rack, int superInRack, double cpu, double mem, int numPorts,
            Map<String, SupervisorDetails> sups) {

        List<Number> ports = new LinkedList<>();
        for (int p = 0; p < numPorts; p++) {
            ports.add(p);
        }
        String superId = String.format("r%03ds%03d", rack, superInRack);
        String hostId  = String.format("host-%03d-rack-%03d", superInRack, rack);
        Map<String, Double> resourceMap = new HashMap<>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, cpu);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, mem);
        resourceMap.put("network.resource.units", 50.0);
        SupervisorDetails sup = new SupervisorDetails(superId,
                hostId, null, ports,
                NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(resourceMap));
        sups.put(sup.getId(), sup);
    }

    /**
     * Create supervisors.
     *
     * @param uniformSupervisors true if all supervisors are of the same size, false otherwise.
     * @return supervisor details indexed by id
     */
    private static Map<String, SupervisorDetails> createSupervisors(boolean uniformSupervisors) {
        Map<String, SupervisorDetails> retVal;
        if (uniformSupervisors) {
            int numRacks = 16;
            int numSupersPerRack = 82;
            int numPorts = 50; // note: scheduling is slower when components with large cpu/mem leave large percent of workerslots unused
            int rackStart = 0;
            int superInRackStart = 1;
            double cpu = 7200; // %percent
            double mem = 356_000; // MB
            Map<String, Double> miscResources = new HashMap<>();
            miscResources.put("network.resource.units", 100.0);

            return TestUtilsForResourceAwareScheduler.genSupervisorsWithRacks(
                    numRacks, numSupersPerRack, numPorts, rackStart, superInRackStart, cpu, mem, miscResources);

        } else {
            // this non-uniform supervisor distribution closely (but not exactly) mimics a large cluster in use
            int numSupersPerRack = 82;
            int numPorts = 50;

            Map<String, SupervisorDetails> retList = new HashMap<>();

            for (int rack = 0 ; rack < 12 ; rack++) {
                double cpu = 3600; // %percent
                double mem = 178_000; // MB
                for (int superInRack = 0; superInRack < numSupersPerRack ; superInRack++) {
                    createAndAddOneSupervisor(rack, superInRack, cpu - 100 * (superInRack % 2), mem, numPorts, retList);
                }
            }
            for (int rack = 12 ; rack < 14 ; rack++) {
                double cpu = 2400; // %percent
                double mem = 118_100; // MB
                for (int superInRack = 0; superInRack < numSupersPerRack ; superInRack++) {
                    createAndAddOneSupervisor(rack, superInRack, cpu - 100 * (superInRack % 2), mem, numPorts, retList);
                }
            }
            for (int rack = 14 ; rack < 16 ; rack++) {
                double cpu = 1200; // %percent
                double mem = 42_480; // MB
                for (int superInRack = 0; superInRack < numSupersPerRack ; superInRack++) {
                    createAndAddOneSupervisor(rack, superInRack, cpu - 100 * (superInRack % 2), mem, numPorts, retList);
                }
            }
            return retList;
        }
    }

    /**
     * Create a large cluster, read topologies and configuration from resource directory and schedule.
     *
     * @throws Exception
     */
    @Test
    public void testLargeCluster() throws Exception {
        boolean uniformSupervisors = false; // false means non-uniform supervisor distribution

        Map<String, SupervisorDetails> supervisors = createSupervisors(uniformSupervisors);

        TopologyDetails[] topoDetailsArray = createTopoDetailsArray(false);
        Assert.assertTrue("No topologies found", topoDetailsArray.length > 0);
        Topologies topologies = new Topologies(topoDetailsArray);

        Config confWithDefaultStrategy = new Config();
        confWithDefaultStrategy.putAll(topoDetailsArray[0].getConf());
        confWithDefaultStrategy.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, DefaultResourceAwareStrategy.class.getName());

        INimbus iNimbus = new INimbusTest();
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supervisors, new HashMap<>(),
                topologies, confWithDefaultStrategy);

        scheduler = new ResourceAwareScheduler();

        List<Class> classesToDebug = Arrays.asList(DefaultResourceAwareStrategy.class,
                GenericResourceAwareStrategy.class, ResourceAwareScheduler.class,
                Cluster.class
        );
        Level logLevel = Level.INFO ; // switch to Level.DEBUG for verbose otherwise Level.INFO
        classesToDebug.forEach(x -> Configurator.setLevel(x.getName(), logLevel));
        long startTime = System.currentTimeMillis();
        scheduler.prepare(confWithDefaultStrategy, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);
        long endTime = System.currentTimeMillis();
        LOG.info("Scheduling Time: {} topologies in {} seconds", topoDetailsArray.length, (endTime - startTime) / 1000.0);

        for (TopologyDetails td : topoDetailsArray) {
            TestUtilsForResourceAwareScheduler.assertTopologiesFullyScheduled(cluster, td.getName());
        }

        // Remove topology and reschedule it
        for (int i = 0 ; i < topoDetailsArray.length ; i++) {
            startTime = System.currentTimeMillis();
            TopologyDetails topoDetails = topoDetailsArray[i];
            cluster.unassign(topoDetails.getId());
            LOG.info("({}) Removed topology {}", i, topoDetails.getName());
            IScheduler rescheduler = new ResourceAwareScheduler();
            rescheduler.prepare(confWithDefaultStrategy, new StormMetricsRegistry());
            rescheduler.schedule(topologies, cluster);
            TestUtilsForResourceAwareScheduler.assertTopologiesFullyScheduled(cluster, topoDetails.getName());
            endTime = System.currentTimeMillis();
            LOG.info("({}) Scheduling Time: Removed topology {} and rescheduled in {} seconds", i, topoDetails.getName(), (endTime - startTime) / 1000.0);
        }
        classesToDebug.forEach(x -> Configurator.setLevel(x.getName(), Level.INFO));
    }

    public static class INimbusTest implements INimbus {
        @Override
        public void prepare(Map<String, Object> topoConf, String schedulerLocalDir) {

        }

        @Override
        public Collection<WorkerSlot> allSlotsAvailableForScheduling(Collection<SupervisorDetails> existingSupervisors,
                                                                     Topologies topologies, Set<String> topologiesMissingAssignments) {
            //return null;
            Set<WorkerSlot> ret = new HashSet<>();
            for (SupervisorDetails sd : existingSupervisors) {
                String id = sd.getId();
                for (Number port : (Collection<Number>) sd.getMeta()) {
                    ret.add(new WorkerSlot(id, port));
                }
            }
            return ret;
        }

        @Override
        public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> newSlotsByTopologyId) {

        }

        @Override
        public String getHostName(Map<String, SupervisorDetails> existingSupervisors, String nodeId) {
            if (existingSupervisors.containsKey(nodeId)) {
                return existingSupervisors.get(nodeId).getHost();
            }
            return null;
        }

        @Override
        public IScheduler getForcedScheduler() {
            return null;
        }
    }
}
