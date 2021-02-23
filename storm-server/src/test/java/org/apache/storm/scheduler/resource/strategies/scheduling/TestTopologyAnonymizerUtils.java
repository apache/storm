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

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.serialization.GzipThriftSerializationDelegate;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Anonymize Serialized Topologies and Configs with the goal of taking internally developed topologies and configuration
 * and make them publicly available for testing.
 *
 * Assume that topologies and configurations exist in the specified resource directory with names ending in
 * {@link #COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING} and {@link #COMPRESSED_SERIALIZED_CONFIG_FILENAME_ENDING}
 * respectively as they exist in blobstore. Also, when both these files exist for the same topology,
 * they share the same file name prefix.
 *
 * <li> Rename topologies and its corresponding configuration (as identified by its resource name). Ensure that renamed
 * configuration file for a topology retains the proper linkage so that:
 *     <p>&lt;old-topo-name&gt;-stormcode.ser -&gt; &lt;new-topo-name&gt;-stormcode.ser</p> and its old conf
 *     <p>&lt;old-topo-name&gt;-stormconf.ser -&gt; &lt;new-topo-name&gt;-stormconf.ser</p>
 * </li>
 *
 * <li>Rename components in each of the topologies.</li>
 *
 * The new converted resource files can be copied to a resource directory under "clusterconf" and made available for use
 * in TestLargeCluster class.
 */
public class TestTopologyAnonymizerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestTopologyAnonymizerUtils.class);

    private static final String DEFAULT_ORIGINAL_RESOURCES_PATH = "clusterconf/ebonyred";
    private static final String DEFAULT_ANONYMIZED_RESOURCES_OUTDIR = "src/test/resources/clusterconf/largeCluster03";
    public static final String COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING = "stormcode.ser";
    public static final String COMPRESSED_SERIALIZED_CONFIG_FILENAME_ENDING = "stormconf.ser";

    private String originalResourcePath;
    private String outputDirPath;

    public TestTopologyAnonymizerUtils() {
        this.originalResourcePath = DEFAULT_ORIGINAL_RESOURCES_PATH;
        this.outputDirPath = DEFAULT_ANONYMIZED_RESOURCES_OUTDIR;
    }

    /**
     * Check if resource files are available in the resource path defined by originalResourcePath.
     *
     * @throws Exception if there are no resource files in input directory.
     */
    public void testResourceAvailability() throws Exception {
        List<String> resources = getResourceFiles(originalResourcePath);
        if (resources.isEmpty()) {
            throw new Exception("No resource files found in resource path " + originalResourcePath);
        }
    }

    /**
     * Take all compressed serialized files in {@link #originalResourcePath} and create anonymized versions or the
     * topology and configuration in the {@link #outputDirPath}.
     *
     * @throws Exception
     */
    public void anonymizeDirectory() throws Exception {
        Map<String, Integer> seenTopoNameIndex = new HashMap<>();
        List<String> errs = new ArrayList<>();

        List<String> resources = getResourceFiles(originalResourcePath);
        for (String resource : resources) {
            if (resource.length() <= COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING.length()) {
                String err = String.format("Resource %s name is too short", resource);
                errs.add(err);
                LOG.error(err);
                continue;
            }
            String resType = resource.substring(resource.length() - COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING.length());
            String entryName = getEntryName(
                    resource.substring(0, resource.length() - COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING.length()),
                    seenTopoNameIndex);
            int entryNum = seenTopoNameIndex.get(entryName);
            String topoName = String.format("TopologyName%05d", entryNum);
            String topoId = String.format("TopologyId%05d", entryNum);
            String newResourceName = String.format("%s-%s", topoName, resType);

            switch (resType) {
                case COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING:
                    // anonymize StormTopology
                    LOG.info("Anonymizing Topology {} as {}, with topoId={}", resource, newResourceName, topoId);
                    StormTopology stormTopology = readAndAnonymizeTopology(resource, errs);
                    writeCompressedResource(newResourceName, new GzipThriftSerializationDelegate().serialize(stormTopology));
                    break;

                case COMPRESSED_SERIALIZED_CONFIG_FILENAME_ENDING:
                    // anonymize config
                    LOG.info("Anonymizing Config {} as {}", resource, newResourceName);
                    Map<String, Object> conf = readAndAnonymizeConfig(resource, topoName, errs);
                    writeCompressedResource(newResourceName, Utils.toCompressedJsonConf(conf));
                    break;

                default:
                    String err = String.format("Resource %s is not recognized as one of supported types", resource);
                    errs.add(err);
                    LOG.warn(err);
            }
        }
        if (!errs.isEmpty()) {
            throw new Exception("Unable to parse all serialized objects\n\t" + String.join("\n\t", errs));
        }
    }

    /**
     * InputStream to read the fully qualified resource path.
     *
     * @param resourcePath
     * @return
     */
    private static InputStream getResourceAsStream(String resourcePath) {
        final InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);
        return in == null ? ClassLoader.getSystemClassLoader().getResourceAsStream(resourcePath) : in;
    }

    /**
     * Get the list of serialized topology (ending with {@link #COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING}
     * and configuration (ending with {@link #COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING})
     * resource files in the path.
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
     * Read the contents of the fully qualified resource path.
     *
     * @param resourcePath
     * @return
     * @throws Exception
     */
    private static byte[] getResourceAsBytes(String resourcePath) throws Exception {
        InputStream in = getResourceAsStream(resourcePath);
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

    private String getEntryName(String resourceNamePrefix, Map<String, Integer> seenTopoNameIndex) {
        int lastIdxOfSlash = resourceNamePrefix.lastIndexOf("/");
        String baseName = resourceNamePrefix.substring(lastIdxOfSlash + 1);
        seenTopoNameIndex.putIfAbsent(baseName, seenTopoNameIndex.size());
        return baseName;
    }

    private StormTopology readAndAnonymizeTopology(String resource, List<String> errors) {
        StormTopology stormTopology;
        try {
            stormTopology = Utils.deserialize(getResourceAsBytes(resource), StormTopology.class);
        } catch (Exception ex) {
            String err = String.format("Cannot read topology from resource %s", resource);
            errors.add(err);
            LOG.error(err, ex);
            return null;
        }

        Map<String, String> renameMap = new HashMap<>();
        if (stormTopology.get_spouts() != null){
            for (String name : stormTopology.get_spouts().keySet()) {
                String newName = String.format("Spout-%d", renameMap.size());
                renameMap.putIfAbsent(name, newName);
            }
        }
        int spoutCnt = renameMap.size();
        if (stormTopology.get_bolts() != null) {
            for (String name : stormTopology.get_bolts().keySet()) {
                String newName = String.format("Bolt-%d", renameMap.size() - spoutCnt);
                renameMap.putIfAbsent(name, newName);
            }
        }
        int boltCnt = renameMap.size() - spoutCnt;
        // rename components
        StormTopology retVal = stormTopology.deepCopy();
        if (spoutCnt > 0) {
            Map<String, SpoutSpec> spouts = retVal.get_spouts();
            for (String name: renameMap.keySet()) {
                if (spouts.containsKey(name)) {
                    spouts.put(renameMap.get(name), spouts.remove(name));
                }
            }
            retVal.get_spouts().values().forEach(spec -> {
                for (GlobalStreamId globalId : spec.get_common().get_inputs().keySet()) {
                    if (renameMap.containsKey(globalId.get_componentId())) {
                        globalId.set_componentId(renameMap.get(globalId.get_componentId()));
                    }
                }
            });
        }

        if (boltCnt > 0) {
            Map<String, Bolt> bolts = retVal.get_bolts();
            for (String name: renameMap.keySet()) {
                if (bolts.containsKey(name)) {
                    bolts.put(renameMap.get(name), bolts.remove(name));
                }
            }
            retVal.get_bolts().values().forEach(spec -> {
                for (GlobalStreamId globalId : spec.get_common().get_inputs().keySet()) {
                    if (renameMap.containsKey(globalId.get_componentId())) {
                        globalId.set_componentId(renameMap.get(globalId.get_componentId()));
                    }
                }
            });
        }
        return retVal;
    }

    private Map<String, Object> readAndAnonymizeConfig(String confResource, String topoName, List<String> errors) {
        Map<String, Object> conf;
        try {
            conf = Utils.fromCompressedJsonConf(getResourceAsBytes(confResource));
        } catch (Exception ex) {
            String err = String.format("Cannot read configuration from resource %s", confResource);
            errors.add(err);
            LOG.error(err, ex);
            return null;
        }

        conf.put(Config.TOPOLOGY_NAME, topoName);
        if (!conf.containsKey(Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER)) {
            conf.put(Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER, false);
        }
        if (!conf.containsKey(Config.TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER)) {
            conf.put(Config.TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER, false);
        }
        // Fix 0.10 topology, config param used by ConstraintSolverStrategy
        if (!conf.containsKey(DaemonConfig.RESOURCE_AWARE_SCHEDULER_MAX_STATE_SEARCH)) {
            conf.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_MAX_STATE_SEARCH, 10_000);
        }
        if (!conf.containsKey(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH)) {
            conf.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, 10_000);
        }
        return conf;
    }

    private void writeCompressedResource(String newResourceName, byte[] compressedBytes) throws IOException {
        File dir = new File(outputDirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        try (FileOutputStream fos = new FileOutputStream(new File(dir, newResourceName))
        ) {
            fos.write(compressedBytes);
        }
    }

    /**
     * In order to create resources as part of a test run:
     *    <li>Download compressed topologies and configurations (from blobstore) into resource path
     *    {@link #DEFAULT_ORIGINAL_RESOURCES_PATH}. The resource names must end with either
     *    {@link #COMPRESSED_SERIALIZED_TOPOLOGY_FILENAME_ENDING} or {@link #COMPRESSED_SERIALIZED_CONFIG_FILENAME_ENDING}</li>
     *    <li>Change pathnames for {@link #DEFAULT_ORIGINAL_RESOURCES_PATH} and {@link #DEFAULT_ANONYMIZED_RESOURCES_OUTDIR}</li>
     *    <li>Uncomment annotation so that this method is executed as a test</li>
     *    <li>add files in {@link #DEFAULT_ANONYMIZED_RESOURCES_OUTDIR} to the resource path "clusterconf/new-cluster-name"</li>
     *    <li>use TestLargeCluster to test these newly generated files after changing
     *    {@link TestLargeCluster#TEST_CLUSTER_NAME} to "new-cluster-name"</li>
     *    
     * @throws Exception
     */
    // @Test
    public void testAnonymizer() throws Exception {
        String[] args = { DEFAULT_ORIGINAL_RESOURCES_PATH, DEFAULT_ANONYMIZED_RESOURCES_OUTDIR };
        TestTopologyAnonymizerUtils instance = new TestTopologyAnonymizerUtils();
        instance.originalResourcePath = args[0];
        instance.outputDirPath = args[1];
        instance.testResourceAvailability();
        instance.anonymizeDirectory();
        LOG.info("Read resources in {} and wrote anonymized files to {}", instance.originalResourcePath, instance.outputDirPath);
    }

    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            args = new String[]{DEFAULT_ORIGINAL_RESOURCES_PATH, DEFAULT_ANONYMIZED_RESOURCES_OUTDIR};
        }
        if (args.length != 2) {
            LOG.error("Expecting two arguments <sourceResourcePath> <targetDir>, received {} args", args.length);
            System.exit(-1);
        }

        TestTopologyAnonymizerUtils instance = new TestTopologyAnonymizerUtils();
        instance.originalResourcePath = args[0];
        instance.outputDirPath = args[1];
        try {
            instance.testResourceAvailability();
            instance.anonymizeDirectory();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        }
    }
}
