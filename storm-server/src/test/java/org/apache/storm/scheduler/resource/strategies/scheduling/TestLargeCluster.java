/*
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
import java.util.concurrent.atomic.AtomicInteger;

@ExtendWith({NormalizedResourcesExtension.class})
public class TestLargeCluster {
    private static final Logger LOG = LoggerFactory.getLogger(TestLargeCluster.class);

    public enum TEST_CLUSTER_NAME {
        TEST_CLUSTER_01("largeCluster01"),
        TEST_CLUSTER_02("largeCluster02"),
        TEST_CLUSTER_03("largeCluster03");

        private final String clusterName;

        TEST_CLUSTER_NAME(String clusterName) {
            this.clusterName = clusterName;
        }

        String getClusterName() {
            return clusterName;
        }

        String getResourcePath() {
            return "clusterconf/" + clusterName;
        }
    }

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
     * @return list of resource file names
     * @throws IOException upon exception in reading resources.
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
     * @param resource path to read.
     * @return InputStream of the resource being read.
     */
    public static InputStream getResourceAsStream(String resource) {
        final InputStream in = getContextClassLoader().getResourceAsStream(resource);
        return in == null ? ClassLoader.getSystemClassLoader().getResourceAsStream(resource) : in;
    }

    /**
     * Read the contents of the fully qualified resource path.
     *
     * @param resource to read.
     * @return byte array of the fully read resource.
     * @throws Exception upon error in reading resource.
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
     * resource path. Skip topologies with no executors/components.
     *
     * @param failOnParseError throw exception if there are unmatched files, otherwise ignore unmatched and read errors.
     * @return An array of TopologyDetails representing resource files.
     * @throws Exception upon error in reading topology serialized files.
     */
    public static TopologyDetails[] createTopoDetailsArray(String resourcePath, boolean failOnParseError) throws Exception {
        List<TopologyDetails> topoDetailsList = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        List<String> resources = getResourceFiles(resourcePath);
        Map<String, String> codeResourceMap = new TreeMap<>();
        Map<String, String> confResourceMap = new HashMap<>();
        for (String resource : resources) {
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

        for (String topoId : codeResourceMap.keySet()) {
            String codeResource = codeResourceMap.get(topoId);
            if (!confResourceMap.containsKey(topoId)) {
                String err = String.format("Ignoring topology file %s because of missing config file for %s", codeResource, topoId);
                errors.add(err);
                LOG.error(err);
                continue;
            }
            String confResource = confResourceMap.get(topoId);
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
            if (!conf.containsKey(Config.TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER)) {
                conf.put(Config.TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER, 1);
            }

            String topoName = (String) conf.getOrDefault(Config.TOPOLOGY_NAME, topoId);

            // conf
            StringBuilder sb = new StringBuilder("Config for " + topoId + ": ");
            for (String param : examinedConfParams) {
                Object val = conf.getOrDefault(param, "<null>");
                sb.append(param).append("=").append(val).append(", ");
            }
            LOG.info(sb.toString());

            // topo
            Map<ExecutorDetails, String> execToComp = TestUtilsForResourceAwareScheduler.genExecsAndComps(stormTopology);
            LOG.info("Topology \"{}\" spouts={}, bolts={}, execToComp size is {}", topoName,
                    stormTopology.get_spouts_size(), stormTopology.get_bolts_size(), execToComp.size());
            if (execToComp.isEmpty()) {
                LOG.error("Topology \"{}\" Ignoring BAD topology with zero executors", topoName);
                continue;
            }
            int numWorkers = Integer.parseInt("" + conf.getOrDefault(Config.TOPOLOGY_WORKERS, "0"));
            TopologyDetails topo = new TopologyDetails(topoId, conf, stormTopology,  numWorkers,
                    execToComp, Time.currentTimeSecs(), "user");
            topo.getUserTopolgyComponents(); // sanity check - normally this should not fail

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
        for (TEST_CLUSTER_NAME testClusterName: TEST_CLUSTER_NAME.values()) {
            String resourcePath = testClusterName.getResourcePath();
            List<String> resources = getResourceFiles(resourcePath);
            Assert.assertFalse("No resource files found in " + resourcePath, resources.isEmpty());
            createTopoDetailsArray(resourcePath, true);
        }
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
     * Create supervisors based on a predefined supervisor distribution modeled after an existing
     * large cluster in use.
     *
     * @param testClusterName cluster for which the supervisors are created.
     * @param reducedSupervisorsPerRack number of supervisors to reduce per rack.
     * @return created supervisors.
     */
    private static Map<String, SupervisorDetails> createSupervisors(
        TEST_CLUSTER_NAME testClusterName, int reducedSupervisorsPerRack) {

        Collection<SupervisorDistribution> supervisorDistributions = SupervisorDistribution.getSupervisorDistribution(testClusterName);
        Map<String, Collection<SupervisorDistribution>> byRackId = SupervisorDistribution.mapByRackId(supervisorDistributions);
        LOG.info("Cluster={}, Designed capacity: {}",
            testClusterName.getClusterName(), SupervisorDistribution.clusterCapacity(supervisorDistributions));

        Map<String, SupervisorDetails> retList = new HashMap<>();
        Map<String, AtomicInteger> seenRacks = new HashMap<>();
        byRackId.forEach((rackId, list) -> {
            int tmpRackSupervisorCnt = list.stream().mapToInt(x -> x.supervisorCnt).sum();
            if (tmpRackSupervisorCnt > Math.abs(reducedSupervisorsPerRack)) {
                tmpRackSupervisorCnt -= Math.abs(reducedSupervisorsPerRack);
            }
            final int adjustedRackSupervisorCnt = tmpRackSupervisorCnt;
            list.forEach(x -> {
                int supervisorCnt = x.supervisorCnt;
                for (int i = 0; i < supervisorCnt; i++) {
                    int superInRack = seenRacks.computeIfAbsent(rackId, z -> new AtomicInteger(-1)).incrementAndGet();
                    int rackNum = seenRacks.size() - 1;
                    if (superInRack >= adjustedRackSupervisorCnt) {
                        continue;
                    }
                    createAndAddOneSupervisor(rackNum, superInRack, x.cpuPercent, x.memoryMb, x.slotCnt, retList);
                }
            });
        });
        return retList;
    }

    /**
     * Create a large cluster, read topologies and configuration from resource directory and schedule.
     *
     * @throws Exception upon error.
     */
    @Test
    public void testLargeCluster() throws Exception {
        for (TEST_CLUSTER_NAME testClusterName: TEST_CLUSTER_NAME.values()) {
            LOG.info("********************************************");
            LOG.info("testLargeCluster: Start Processing cluster {}", testClusterName.getClusterName());

            String resourcePath = testClusterName.getResourcePath();
            Map<String, SupervisorDetails> supervisors = createSupervisors(testClusterName, 0);

            TopologyDetails[] topoDetailsArray = createTopoDetailsArray(resourcePath, false);
            Assert.assertTrue("No topologies found for cluster " + testClusterName.getClusterName(), topoDetailsArray.length > 0);
            Topologies topologies = new Topologies(topoDetailsArray);

            Config confWithDefaultStrategy = new Config();
            confWithDefaultStrategy.putAll(topoDetailsArray[0].getConf());
            confWithDefaultStrategy.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, DefaultResourceAwareStrategy.class.getName());
            confWithDefaultStrategy.put(
                Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN,
                TestUtilsForResourceAwareScheduler.GenSupervisorsDnsToSwitchMapping.class.getName());

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
            LOG.info("Cluster={} Scheduling Time: {} topologies in {} seconds",
                testClusterName.getClusterName(), topoDetailsArray.length, (endTime - startTime) / 1000.0);

            for (TopologyDetails td : topoDetailsArray) {
                TestUtilsForResourceAwareScheduler.assertTopologiesFullyScheduled(cluster, td.getName());
            }

            // Remove topology and reschedule it
            for (int i = 0 ; i < topoDetailsArray.length ; i++) {
                startTime = System.currentTimeMillis();
                TopologyDetails topoDetails = topoDetailsArray[i];
                cluster.unassign(topoDetails.getId());
                LOG.info("Cluster={},  ({}) Removed topology {}", testClusterName.getClusterName(), i, topoDetails.getName());
                IScheduler rescheduler = new ResourceAwareScheduler();
                rescheduler.prepare(confWithDefaultStrategy, new StormMetricsRegistry());
                rescheduler.schedule(topologies, cluster);
                TestUtilsForResourceAwareScheduler.assertTopologiesFullyScheduled(cluster, topoDetails.getName());
                endTime = System.currentTimeMillis();
                LOG.info("Cluster={}, ({}) Scheduling Time: Removed topology {} and rescheduled in {} seconds",
                    testClusterName.getClusterName(), i, topoDetails.getName(), (endTime - startTime) / 1000.0);
            }
            classesToDebug.forEach(x -> Configurator.setLevel(x.getName(), Level.INFO));

            LOG.info("testLargeCluster: End Processing cluster {}", testClusterName.getClusterName());
            LOG.info("********************************************");
        }
    }

    public static class SupervisorDistribution {
        final String rackId;
        final int supervisorCnt;
        final int slotCnt;
        final int memoryMb;
        final int cpuPercent;

        public SupervisorDistribution(int supervisorCnt, String rackId, int slotCnt, int memoryMb, int cpuPercent) {
            this.rackId = rackId;
            this.supervisorCnt = supervisorCnt;
            this.slotCnt = slotCnt;
            this.memoryMb = memoryMb;
            this.cpuPercent = cpuPercent;
        }

        public static Map<String, Collection<SupervisorDistribution>> mapByRackId(Collection<SupervisorDistribution> supervisors) {
            Map<String, Collection<SupervisorDistribution>> retVal = new HashMap<>();
            supervisors.forEach(x -> retVal.computeIfAbsent(x.rackId, rackId -> new ArrayList<>()).add(x));
            return retVal;
        }

        public static Collection<SupervisorDistribution> getSupervisorDistribution(TEST_CLUSTER_NAME testClusterName) {
            switch (testClusterName) {
                case TEST_CLUSTER_01:
                    return getSupervisorDistribution01();
                case TEST_CLUSTER_02:
                    return getSupervisorDistribution02();
                case TEST_CLUSTER_03:
                default:
                    return getSupervisorDistribution03();
            }
        }

        private static Collection<SupervisorDistribution> getSupervisorDistribution01() {
            int numSupersPerRack = 82;
            int numPorts = 50;
            int numSupersPerRackEven = numSupersPerRack / 2;
            int numSupersPerRackOdd = numSupersPerRack - numSupersPerRackEven;

            List<SupervisorDistribution> ret = new ArrayList<>();

            for (int rack = 0; rack < 12; rack++) {
                String rackId = String.format("r%03d", rack);
                int cpu = 3600; // %percent
                int mem = 178_000; // MB
                int adjustedCpu = cpu - 100;
                ret.add(new SupervisorDistribution(numSupersPerRackEven, rackId, numPorts, mem, cpu));
                ret.add(new SupervisorDistribution(numSupersPerRackOdd, rackId, numPorts, mem, adjustedCpu));
            }
            for (int rack = 12; rack < 14; rack++) {
                String rackId = String.format("r%03d", rack);
                int cpu = 2400; // %percent
                int mem = 118_100; // MB
                int adjustedCpu = cpu - 100;
                ret.add(new SupervisorDistribution(numSupersPerRackEven, rackId, numPorts, mem, cpu));
                ret.add(new SupervisorDistribution(numSupersPerRackOdd, rackId, numPorts, mem, adjustedCpu));
            }
            for (int rack = 14; rack < 16; rack++) {
                String rackId = String.format("r%03d", rack);
                int cpu = 1200; // %percent
                int mem = 42_480; // MB
                int adjustedCpu = cpu - 100;
                ret.add(new SupervisorDistribution(numSupersPerRackEven, rackId, numPorts, mem, cpu));
                ret.add(new SupervisorDistribution(numSupersPerRackOdd, rackId, numPorts, mem, adjustedCpu));
            }
            return ret;
        }

        public static Collection<SupervisorDistribution> getSupervisorDistribution02() {
            return Arrays.asList(
                // Cnt, Rack,    Slot, Mem, CPU
                new SupervisorDistribution(78, "r001", 12, 42461, 1100),
                new SupervisorDistribution(146, "r002", 36, 181362, 3500),
                new SupervisorDistribution(18, "r003", 36, 181362, 3500),
                new SupervisorDistribution(120, "r004", 36, 181362, 3500),
                new SupervisorDistribution(24, "r005", 36, 181362, 3500),
                new SupervisorDistribution(16, "r005", 48, 177748, 4700),
                new SupervisorDistribution(12, "r006", 18, 88305, 1800),
                new SupervisorDistribution(368, "r006", 36, 181205, 3500),
                new SupervisorDistribution(62, "r007", 48, 177748, 4700),
                new SupervisorDistribution(50, "r008", 36, 181348, 3500),
                new SupervisorDistribution(64, "r008", 48, 177748, 4700),
                new SupervisorDistribution(74, "r009", 48, 177748, 4700),
                new SupervisorDistribution(74, "r010", 48, 177748, 4700),
                new SupervisorDistribution(10, "r011", 48, 177748, 4700),
                new SupervisorDistribution(78, "r012", 24, 120688, 2300),
                new SupervisorDistribution(150, "r013", 48, 177748, 4700),
                new SupervisorDistribution(76, "r014", 36, 181362, 3500),
                new SupervisorDistribution(38, "r015", 48, 174431, 4700),
                new SupervisorDistribution(78, "r016", 36, 181375, 3500),
                new SupervisorDistribution(72, "r017", 36, 181362, 3500),
                new SupervisorDistribution(80, "r018", 36, 181362, 3500),
                new SupervisorDistribution(76, "r019", 36, 181362, 3500),
                new SupervisorDistribution(78, "r020", 24, 120696, 2300),
                new SupervisorDistribution(80, "r021", 24, 120696, 2300)
            );
        }

        public static Collection<SupervisorDistribution> getSupervisorDistribution03() {
            return Arrays.asList(
                // Cnt, Rack,    Slot, Mem, CPU
                new SupervisorDistribution(40, "r001", 12, 58829, 1100),
                new SupervisorDistribution(40, "r002", 12, 58829, 1100)
            );
        }

        public static String clusterCapacity(Collection<SupervisorDistribution> supervisorDistributions) {
            long cpuPercent = 0;
            long memoryMb = 0;
            int supervisorCnt = 0;
            Set<String> racks = new HashSet<>();

            for (SupervisorDistribution x: supervisorDistributions) {
                memoryMb += ((long) x.supervisorCnt * x.memoryMb);
                cpuPercent += ((long) x.supervisorCnt * x.cpuPercent);
                supervisorCnt += x.supervisorCnt;
                racks.add(x.rackId);
            }
            return String.format("Cluster summary: Racks=%d, Supervisors=%d, memoryMb=%d, cpuPercent=%d",
                racks.size(), supervisorCnt, memoryMb, cpuPercent);
        }
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
