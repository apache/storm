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

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.validation.ConfigValidation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ServerConfigUtils {
    public static final String FILE_SEPARATOR = File.separator;
    public final static String NIMBUS_DO_NOT_REASSIGN = "NIMBUS-DO-NOT-REASSIGN";
    public final static String RESOURCES_SUBDIR = "resources";

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static ServerConfigUtils _instance = new ServerConfigUtils();

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     * @param u a ServerConfigUtils instance
     * @return the previously set instance
     */
    public static ServerConfigUtils setInstance(ServerConfigUtils u) {
        ServerConfigUtils oldInstance = _instance;
        _instance = u;
        return oldInstance;
    }


    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static Map<String, Object> readStormConfig() {
        return _instance.readStormConfigImpl();
    }

    public static String masterLocalDir(Map conf) throws IOException {
        String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR)) + FILE_SEPARATOR + "nimbus";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String masterInimbusDir(Map conf) throws IOException {
        return (masterLocalDir(conf) + FILE_SEPARATOR + "inimbus");
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static LocalState nimbusTopoHistoryState(Map conf) throws IOException {
        return _instance.nimbusTopoHistoryStateImpl(conf);
    }

    public static String masterInbox(Map conf) throws IOException {
        String ret = masterLocalDir(conf) + FILE_SEPARATOR + "inbox";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String masterStormDistRoot(Map conf) throws IOException {
        String ret = ConfigUtils.stormDistPath(masterLocalDir(conf));
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    /* TODO: make sure test these two functions in manual tests */
    public static List<String> getTopoLogsUsers(Map topologyConf) {
        List<String> logsUsers = (List<String>)topologyConf.get(DaemonConfig.LOGS_USERS);
        List<String> topologyUsers = (List<String>)topologyConf.get(Config.TOPOLOGY_USERS);
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

    public static List<String> getTopoLogsGroups(Map topologyConf) {
        List<String> logsGroups = (List<String>)topologyConf.get(DaemonConfig.LOGS_GROUPS);
        List<String> topologyGroups = (List<String>)topologyConf.get(Config.TOPOLOGY_GROUPS);
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

    public static String masterStormDistRoot(Map conf, String stormId) throws IOException {
        return (masterStormDistRoot(conf) + FILE_SEPARATOR + stormId);
    }

    public static String supervisorTmpDir(Map conf) throws IOException {
        String ret = ConfigUtils.supervisorLocalDir(conf) + FILE_SEPARATOR + "tmp";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String supervisorIsupervisorDir(Map conf) throws IOException {
        return (ConfigUtils.supervisorLocalDir(conf) + FILE_SEPARATOR + "isupervisor");
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static LocalState supervisorState(Map conf) throws IOException {
        return _instance.supervisorStateImpl(conf);
    }

    public static String absoluteHealthCheckDir(Map conf) {
        String stormHome = System.getProperty("storm.home");
        String healthCheckDir = (String) conf.get(DaemonConfig.STORM_HEALTH_CHECK_DIR);
        if (healthCheckDir == null) {
            return (stormHome + FILE_SEPARATOR + "healthchecks");
        } else {
            if (new File(healthCheckDir).isAbsolute()) {
                return healthCheckDir;
            } else {
                return (stormHome + FILE_SEPARATOR + healthCheckDir);
            }
        }
    }

    public static File getLogMetaDataFile(String fname) {
        String[] subStrings = fname.split(FILE_SEPARATOR); // TODO: does this work well on windows?
        String id = subStrings[0];
        Integer port = Integer.parseInt(subStrings[1]);
        return getLogMetaDataFile(Utils.readStormConfig(), id, port);
    }

    public static File getLogMetaDataFile(Map conf, String id, Integer port) {
        String fname = ConfigUtils.workerArtifactsRoot(conf, id, port) + FILE_SEPARATOR + "worker.yaml";
        return new File(fname);
    }

    public static String masterStormJarPath(String stormRoot) {
        return (stormRoot + FILE_SEPARATOR + "stormjar.jar");
    }

    public LocalState supervisorStateImpl(Map conf) throws IOException {
        return new LocalState((ConfigUtils.supervisorLocalDir(conf) + FILE_SEPARATOR + "localstate"));
    }

    public LocalState nimbusTopoHistoryStateImpl(Map conf) throws IOException {
        return new LocalState((masterLocalDir(conf) + FILE_SEPARATOR + "history"));
    }

    public Map<String, Object> readStormConfigImpl() {
        Map<String, Object> conf = Utils.readStormConfig();
        ConfigValidation.validateFields(conf, DaemonConfig.class);
        return conf;
    }
}
