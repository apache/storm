/*
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

package org.apache.storm.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;

public class ServerConfigUtils {
    public static final String NIMBUS_DO_NOT_REASSIGN = "NIMBUS-DO-NOT-REASSIGN";
    public static final Path RESOURCES_SUBDIR = Paths.get("resources");

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static ServerConfigUtils _instance = new ServerConfigUtils();

    /**
     * Provide an instance of this class for delegates to use.  To mock out delegated methods, provide an instance of a subclass that
     * overrides the implementation of the delegated method.
     *
     * @param u a ServerConfigUtils instance
     * @return the previously set instance
     */
    public static ServerConfigUtils setInstance(ServerConfigUtils u) {
        ServerConfigUtils oldInstance = _instance;
        _instance = u;
        return oldInstance;
    }

    public static Path masterLocalDir(Map<String, Object> conf) throws IOException {
        Path ret = ConfigUtils.absoluteStormLocalDir(conf).resolve("nimbus");
        Files.createDirectories(ret);
        return ret;
    }

    public static Path masterInimbusDir(Map<String, Object> conf) throws IOException {
        return masterLocalDir(conf).resolve("inimbus");
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static LocalState nimbusTopoHistoryState(Map<String, Object> conf) throws IOException {
        return _instance.nimbusTopoHistoryStateImpl(conf);
    }

    public static Path masterInbox(Map<String, Object> conf) throws IOException {
        Path ret = masterLocalDir(conf).resolve("inbox");
        Files.createDirectories(ret);
        return ret;
    }

    public static Path masterStormDistRoot(Map<String, Object> conf) throws IOException {
        Path ret = ConfigUtils.stormDistPath(masterLocalDir(conf));
        Files.createDirectories(ret);
        return ret;
    }

    /* TODO: make sure test these two functions in manual tests */
    public static List<String> getTopoLogsUsers(Map<String, Object> topologyConf) {
        List<String> logsUsers = (List<String>) topologyConf.get(DaemonConfig.LOGS_USERS);
        List<String> topologyUsers = (List<String>) topologyConf.get(Config.TOPOLOGY_USERS);
        Set<String> mergedUsers = new HashSet<String>();
        if (logsUsers != null) {
            for (String user : logsUsers) {
                if (user != null) {
                    mergedUsers.add(user);
                }
            }
        }
        if (topologyUsers != null) {
            for (String user : topologyUsers) {
                if (user != null) {
                    mergedUsers.add(user);
                }
            }
        }
        List<String> ret = new ArrayList<String>(mergedUsers);
        Collections.sort(ret);
        return ret;
    }

    public static List<String> getTopoLogsGroups(Map<String, Object> topologyConf) {
        List<String> logsGroups = (List<String>) topologyConf.get(DaemonConfig.LOGS_GROUPS);
        List<String> topologyGroups = (List<String>) topologyConf.get(Config.TOPOLOGY_GROUPS);
        Set<String> mergedGroups = new HashSet<String>();
        if (logsGroups != null) {
            for (String group : logsGroups) {
                if (group != null) {
                    mergedGroups.add(group);
                }
            }
        }
        if (topologyGroups != null) {
            for (String group : topologyGroups) {
                if (group != null) {
                    mergedGroups.add(group);
                }
            }
        }
        List<String> ret = new ArrayList<String>(mergedGroups);
        Collections.sort(ret);
        return ret;
    }

    public static Path masterStormDistRoot(Map<String, Object> conf, String stormId) throws IOException {
        return masterStormDistRoot(conf).resolve(stormId);
    }

    public static Path supervisorTmpDir(Map<String, Object> conf) throws IOException {
        Path ret = ConfigUtils.supervisorLocalDir(conf).resolve("tmp");
        Files.createDirectories(ret);
        return ret;
    }

    public static Path supervisorIsupervisorDir(Map<String, Object> conf) throws IOException {
        return ConfigUtils.supervisorLocalDir(conf).resolve("isupervisor");
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static LocalState supervisorState(Map<String, Object> conf) throws IOException {
        return _instance.supervisorStateImpl(conf);
    }

    public static Path absoluteHealthCheckDir(Map<String, Object> conf) {
        Path stormHome = Paths.get(System.getProperty(ConfigUtils.STORM_HOME));
        String healthCheckDir = (String) conf.get(DaemonConfig.STORM_HEALTH_CHECK_DIR);
        if (healthCheckDir == null) {
            return stormHome.resolve("healthchecks");
        } else {
            Path healthCheckDirPath = Paths.get(healthCheckDir);
            if (healthCheckDirPath.isAbsolute()) {
                return healthCheckDirPath;
            } else {
                return stormHome.resolve(healthCheckDir);
            }
        }
    }

    public static Path getLogMetaDataFile(String fname) {
        Path filePath = Paths.get(fname);
        String id = filePath.getName(0).toString();
        Integer port = Integer.parseInt(filePath.getName(1).toString());
        return getLogMetaDataFile(Utils.readStormConfig(), id, port);
    }

    public static Path getLogMetaDataFile(Map<String, Object> conf, String id, Integer port) {
        Path fname = ConfigUtils.workerArtifactsRoot(conf, id, port).resolve("worker.yaml");
        return fname;
    }

    public static Path masterStormJarPath(Path stormRoot) {
        return stormRoot.resolve("stormjar.jar");
    }

    public LocalState supervisorStateImpl(Map<String, Object> conf) throws IOException {
        return new LocalState(ConfigUtils.supervisorLocalDir(conf).resolve("localstate"),  true);
    }

    public LocalState nimbusTopoHistoryStateImpl(Map<String, Object> conf) throws IOException {
        return new LocalState(masterLocalDir(conf).resolve("history"), true);
    }
}
