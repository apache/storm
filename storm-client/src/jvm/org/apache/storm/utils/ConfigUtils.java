/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.utils;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.apache.storm.Config;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.validation.ConfigValidation;
import org.apache.storm.validation.ConfigValidationAnnotations;

public class ConfigUtils {
    public static final String FILE_SEPARATOR = File.separator;
    public static final String STORM_HOME = "storm.home";
    public static final String RESOURCES_SUBDIR = "resources";

    private static final Set<String> passwordConfigKeys = new HashSet<>();

    static {
        for (Class<?> clazz: ConfigValidation.getConfigClasses()) {
            for (Field field : clazz.getFields()) {
                for (Annotation annotation : field.getAnnotations()) {
                    boolean isPassword = annotation.annotationType().getName().equals(
                            ConfigValidationAnnotations.Password.class.getName());
                    if (isPassword) {
                        try {
                            passwordConfigKeys.add((String) field.get(null));
                        } catch (IllegalAccessException e) {
                            // ignore
                        }
                    }
                }
            }
        }
    }


    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static ConfigUtils instance = new ConfigUtils();

    /**
     * Provide an instance of this class for delegates to use.  To mock out delegated methods, provide an instance of a subclass that
     * overrides the implementation of the delegated method.
     *
     * @param u a ConfigUtils instance
     * @return the previously set instance
     */
    public static ConfigUtils setInstance(ConfigUtils u) {
        ConfigUtils oldInstance = instance;
        instance = u;
        return oldInstance;
    }

    public static Map<String, Object> maskPasswords(final Map<String, Object> conf) {
        Maps.EntryTransformer<String, Object, Object> maskPasswords = new Maps.EntryTransformer<String, Object, Object>() {
            @Override
            public Object transformEntry(String key, Object value) {
                return passwordConfigKeys.contains(key) ? "*****" : value;
            }
        };
        return Maps.transformEntries(conf, maskPasswords);
    }

    public static boolean isLocalMode(Map<String, Object> conf) {
        String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
        if (mode != null) {
            if ("local".equals(mode)) {
                return true;
            }
            if ("distributed".equals(mode)) {
                return false;
            }
            throw new IllegalArgumentException("Illegal cluster mode in conf: " + mode);
        }
        return true;
    }

    /**
     * Returns a Collection of file names found under the given directory.
     *
     * @param dir a directory
     * @return the Collection of file names
     */
    public static Collection<String> readDirContents(String dir) {
        Collection<File> ret = readDirFiles(dir);
        return ret.stream().map(car -> car.getName()).collect(Collectors.toList());
    }

    /**
     * Returns a Collection of files found under the given directory.
     *
     * @param dir a directory
     * @return the Collection of file names
     */
    public static Collection<File> readDirFiles(String dir) {
        Collection<File> ret = new HashSet<>();
        File[] files = new File(dir).listFiles();
        if (files != null) {
            for (File f : files) {
                ret.add(f);
            }
        }
        return ret;
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static String workerArtifactsRoot(Map<String, Object> conf) {
        return instance.workerArtifactsRootImpl(conf);
    }

    public static String workerArtifactsRoot(Map<String, Object> conf, String id) {
        return (workerArtifactsRoot(conf) + FILE_SEPARATOR + id);
    }

    public static String workerArtifactsRoot(Map<String, Object> conf, String id, Integer port) {
        return (workerArtifactsRoot(conf, id) + FILE_SEPARATOR + port);
    }

    public static String getLogDir() {
        String dir;
        Map<String, Object> conf;
        if (System.getProperty("storm.log.dir") != null) {
            dir = System.getProperty("storm.log.dir");
        } else if ((conf = readStormConfig()).get("storm.log.dir") != null) {
            dir = String.valueOf(conf.get("storm.log.dir"));
        } else if (System.getProperty(STORM_HOME) != null) {
            dir = System.getProperty(STORM_HOME) + FILE_SEPARATOR + "logs";
        } else {
            dir = "logs";
        }
        try {
            return new File(dir).getCanonicalPath();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Illegal storm.log.dir in conf: " + dir);
        }
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static Map<String, Object> readStormConfig() {
        return instance.readStormConfigImpl();
    }

    public static int samplingRate(Map<String, Object> conf) {
        double rate = ObjectReader.getDouble(conf.get(Config.TOPOLOGY_STATS_SAMPLE_RATE));
        if (rate != 0) {
            return (int) (1 / rate);
        }
        throw new IllegalArgumentException("Illegal topology.stats.sample.rate in conf: " + rate);
    }

    public static BooleanSupplier mkStatsSampler(Map<String, Object> conf) {
        return evenSampler(samplingRate(conf));
    }

    public static BooleanSupplier evenSampler(final int samplingFreq) {
        final Random random = new Random();

        return new BooleanSupplier() {
            private int curr = -1;
            private int target = random.nextInt(samplingFreq);

            @Override
            public boolean getAsBoolean() {
                curr++;
                if (curr >= samplingFreq) {
                    curr = 0;
                    target = random.nextInt(samplingFreq);
                }
                return (curr == target);
            }
        };
    }

    public static StormTopology readSupervisorTopology(Map<String, Object> conf, String stormId, AdvancedFSOps ops) throws IOException {
        return instance.readSupervisorTopologyImpl(conf, stormId, ops);
    }

    public static String supervisorStormCodePath(String stormRoot) {
        return (concatIfNotNull(stormRoot) + FILE_SEPARATOR + "stormcode.ser");
    }

    public static String concatIfNotNull(String dir) {
        String ret = "";
        // we do this since to concat a null String will actually concat a "null", which is not the expected: ""
        if (dir != null) {
            ret = dir;
        }
        return ret;
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static String supervisorStormDistRoot(Map<String, Object> conf) throws IOException {
        return ConfigUtils.instance.supervisorStormDistRootImpl(conf);
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static String supervisorStormDistRoot(Map<String, Object> conf, String stormId) throws IOException {
        return instance.supervisorStormDistRootImpl(conf, stormId);
    }

    public static String sharedByTopologyDir(Map<String, Object> conf, String stormId) throws IOException {
        return supervisorStormDistRoot(conf, stormId) + FILE_SEPARATOR + "shared_by_topology";
    }

    public static String supervisorStormJarPath(String stormRoot) {
        return (concatIfNotNull(stormRoot) + FILE_SEPARATOR + "stormjar.jar");
    }

    public static String supervisorStormConfPath(String stormRoot) {
        return (concatIfNotNull(stormRoot) + FILE_SEPARATOR + "stormconf.ser");
    }

    public static String absoluteStormLocalDir(Map<String, Object> conf) {
        String stormHome = System.getProperty(STORM_HOME);
        String localDir = (String) conf.get(Config.STORM_LOCAL_DIR);
        if (localDir == null) {
            return (stormHome + FILE_SEPARATOR + "storm-local");
        } else {
            if (new File(localDir).isAbsolute()) {
                return localDir;
            } else {
                return (stormHome + FILE_SEPARATOR + localDir);
            }
        }
    }

    public static String absoluteStormBlobStoreDir(Map<String, Object> conf) {
        String blobStoreDir = (String) conf.get(Config.BLOBSTORE_DIR);
        if (blobStoreDir == null) {
            return ConfigUtils.absoluteStormLocalDir(conf);
        } else {
            if (new File(blobStoreDir).isAbsolute()) {
                return blobStoreDir;
            } else {
                String stormHome = System.getProperty(STORM_HOME);
                return (stormHome + FILE_SEPARATOR + blobStoreDir);
            }
        }
    }

    public static StormTopology readSupervisorStormCodeGivenPath(String stormCodePath, AdvancedFSOps ops) throws IOException {
        return Utils.deserialize(ops.slurp(new File(stormCodePath)), StormTopology.class);
    }

    public static String supervisorStormResourcesPath(String stormRoot) {
        return (concatIfNotNull(stormRoot) + FILE_SEPARATOR + RESOURCES_SUBDIR);
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static String workerRoot(Map<String, Object> conf) {
        return instance.workerRootImpl(conf);
    }

    public static String workerRoot(Map<String, Object> conf, String id) {
        return (workerRoot(conf) + FILE_SEPARATOR + id);
    }

    public static String workerArtifactsSymlink(Map<String, Object> conf, String id) {
        return workerRoot(conf, id) + FILE_SEPARATOR + "artifacts";
    }

    public static String workerPidsRoot(Map<String, Object> conf, String id) {
        return (workerRoot(conf, id) + FILE_SEPARATOR + "pids");
    }

    public static String workerPidPath(Map<String, Object> conf, String id, String pid) {
        return (workerPidsRoot(conf, id) + FILE_SEPARATOR + pid);
    }

    public static String workerPidPath(Map<String, Object> conf, String id, long pid) {
        return workerPidPath(conf, id, String.valueOf(pid));
    }

    public static String workerArtifactsPidPath(Map<String, Object> conf, String id, Integer port) {
        return (workerArtifactsRoot(conf, id, port) + FILE_SEPARATOR + "worker.pid");
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static Map<String, Object> readSupervisorStormConf(Map<String, Object> conf, String stormId) throws IOException {
        return instance.readSupervisorStormConfImpl(conf, stormId);
    }

    public static Map<String, Object> readSupervisorStormConfGivenPath(Map<String, Object> conf, String topoConfPath) throws IOException {
        Map<String, Object> ret = new HashMap<>(conf);
        ret.putAll(Utils.fromCompressedJsonConf(FileUtils.readFileToByteArray(new File(topoConfPath))));
        return ret;
    }

    public static Map<String, Object> overrideLoginConfigWithSystemProperty(
        Map<String, Object> conf) { // note that we delete the return value
        String loginConfFile = System.getProperty("java.security.auth.login.config");
        if (loginConfFile != null) {
            conf.put("java.security.auth.login.config", loginConfFile);
        }
        return conf;
    }

    public static String workerHeartbeatsRoot(Map<String, Object> conf, String id) {
        return (workerRoot(conf, id) + FILE_SEPARATOR + "heartbeats");
    }

    public static LocalState workerState(Map<String, Object> conf, String id) throws IOException {
        return new LocalState(workerHeartbeatsRoot(conf, id), false);
    }

    public static String masterStormCodeKey(String topologyId) {
        return (topologyId + "-stormcode.ser");
    }

    public static String masterStormConfKey(String topologyId) {
        return (topologyId + "-stormconf.ser");
    }

    /**
     * Returns the topology ID belonging to a blob key if it exists.
     *
     * @param key the blob key
     * @return the topology id belonging to the key if it can be inferred.  Returns null otherwise.
     */
    public static String getIdFromBlobKey(String key) {
        if (key == null) {
            return null;
        }
        final String stormJarSuffix = "-stormjar.jar";
        final String stormCodeSuffix = "-stormcode.ser";
        final String stormConfSuffix = "-stormconf.ser";

        String ret = null;
        if (key.endsWith(stormJarSuffix)) {
            ret = key.substring(0, key.length() - stormJarSuffix.length());
        } else if (key.endsWith(stormCodeSuffix)) {
            ret = key.substring(0, key.length() - stormCodeSuffix.length());
        } else if (key.endsWith(stormConfSuffix)) {
            ret = key.substring(0, key.length() - stormConfSuffix.length());
        }
        return ret;
    }

    public static String masterStormJarKey(String topologyId) {
        return (topologyId + "-stormjar.jar");
    }

    public static Map<String, Object> readYamlConfig(String name, boolean mustExist) {
        Map<String, Object> conf = Utils.findAndReadConfigFile(name, mustExist);
        ConfigValidation.validateFields(conf);
        return conf;
    }

    public static Map<String, Object> readYamlConfig(String name) {
        return readYamlConfig(name, true);
    }

    public static String stormDistPath(String stormRoot) {
        String ret = "";
        // we do this since to concat a null String will actually concat a "null", which is not the expected: ""
        if (stormRoot != null) {
            ret = stormRoot;
        }
        return ret + FILE_SEPARATOR + "stormdist";
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static String supervisorLocalDir(Map<String, Object> conf) throws IOException {
        return instance.supervisorLocalDirImpl(conf);
    }

    public static String workerTmpRoot(Map<String, Object> conf, String id) {
        return (workerRoot(conf, id) + FILE_SEPARATOR + "tmp");
    }

    public static String workerUserRoot(Map<String, Object> conf) {
        return (absoluteStormLocalDir(conf) + FILE_SEPARATOR + "workers-users");
    }

    public static String workerUserFile(Map<String, Object> conf, String workerId) {
        return (workerUserRoot(conf) + FILE_SEPARATOR + workerId);
    }

    public static File getWorkerDirFromRoot(String logRoot, String id, Integer port) {
        return new File((logRoot + FILE_SEPARATOR + id + FILE_SEPARATOR + port));
    }

    /**
     * Get the given config value as a List &lt;String&gt;, if possible.
     *
     * @param name - the config key
     * @param conf - the config map
     * @return - the config value converted to a List &lt;String&gt; if found, otherwise null.
     *
     * @throws IllegalArgumentException if conf is null
     * @throws NullPointerException     if name is null and the conf map doesn't support null keys
     */
    public static List<String> getValueAsList(String name, Map<String, Object> conf) {
        if (null == conf) {
            throw new IllegalArgumentException("Conf is required");
        }
        Object value = conf.get(name);
        List<String> listValue;
        if (value == null) {
            listValue = null;
        } else if (value instanceof Collection) {
            listValue = ((Collection<?>) value)
                .stream()
                .map(ObjectReader::getString)
                .collect(Collectors.toList());
        } else {
            listValue = Arrays.asList(ObjectReader.getString(value).split("\\s+"));
        }
        return listValue;
    }

    public StormTopology readSupervisorTopologyImpl(Map<String, Object> conf, String stormId, AdvancedFSOps ops) throws IOException {
        String stormRoot = supervisorStormDistRoot(conf, stormId);
        String topologyPath = supervisorStormCodePath(stormRoot);
        return readSupervisorStormCodeGivenPath(topologyPath, ops);
    }

    public Map<String, Object> readStormConfigImpl() {
        Map<String, Object> conf = Utils.readStormConfig();
        ConfigValidation.validateFields(conf);
        return conf;
    }

    public String workerArtifactsRootImpl(Map<String, Object> conf) {
        String artifactsDir = (String) conf.get(Config.STORM_WORKERS_ARTIFACTS_DIR);
        if (artifactsDir == null) {
            return (getLogDir() + FILE_SEPARATOR + "workers-artifacts");
        } else {
            if (new File(artifactsDir).isAbsolute()) {
                return artifactsDir;
            } else {
                return (getLogDir() + FILE_SEPARATOR + artifactsDir);
            }
        }
    }

    public String supervisorStormDistRootImpl(Map<String, Object> conf, String stormId) throws IOException {
        return supervisorStormDistRoot(conf) + FILE_SEPARATOR + Utils.urlEncodeUtf8(stormId);
    }

    public String supervisorStormDistRootImpl(Map<String, Object> conf) throws IOException {
        return stormDistPath(supervisorLocalDir(conf));
    }

    public String workerRootImpl(Map<String, Object> conf) {
        return (absoluteStormLocalDir(conf) + FILE_SEPARATOR + "workers");
    }

    public Map<String, Object> readSupervisorStormConfImpl(Map<String, Object> conf, String stormId) throws IOException {
        String stormRoot = supervisorStormDistRoot(conf, stormId);
        String confPath = supervisorStormConfPath(stormRoot);
        return readSupervisorStormConfGivenPath(conf, confPath);
    }

    public String supervisorLocalDirImpl(Map<String, Object> conf) throws IOException {
        String ret = ConfigUtils.absoluteStormLocalDir(conf) + FILE_SEPARATOR + "supervisor";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }
}
