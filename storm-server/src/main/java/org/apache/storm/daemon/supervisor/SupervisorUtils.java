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

package org.apache.storm.daemon.supervisor;

import static org.apache.storm.ServerConstants.NUMA_PORTS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.localizer.LocalResource;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SupervisorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorUtils.class);

    private static final SupervisorUtils INSTANCE = new SupervisorUtils();
    private static SupervisorUtils _instance = INSTANCE;

    public static void setInstance(SupervisorUtils u) {
        _instance = u;
    }

    public static void resetInstance() {
        _instance = INSTANCE;
    }

    /**
     * getNumaIdForPort for a specific supervisor.
     * @param port port
     * @param supervisorConf supervisorConf
     * @return getNumaIdForPort
     */
    public static String getNumaIdForPort(Integer port, Map<String, Object> supervisorConf) {
        Map<String, Object> validatedNumaMap = getNumaMap(supervisorConf);
        for (Map.Entry<String, Object> numaEntry : validatedNumaMap.entrySet()) {
            Map<String, Object> numaMap  = (Map<String, Object>) numaEntry.getValue();
            List<Integer> portList = (List<Integer>) numaMap.get(NUMA_PORTS);
            if (portList.contains(port)) {
                return numaEntry.getKey();
            }
        }
        return null;
    }

    /**
     * gets the set of all configured numa ports for a specific supervisor.
     * @param supervisorConf supervisorConf
     * @return set of all numa ports
     */
    public static Set<Integer> getNumaPorts(Map<String, Object> supervisorConf) {
        Set<Integer> numaPorts = new HashSet<>();
        Map<String, Object> validatedNumaMap = getNumaMap(supervisorConf);
        for (Map.Entry<String, Object> numaEntry : validatedNumaMap.entrySet()) {
            Map<String, Object> numaMap  = (Map<String, Object>) numaEntry.getValue();
            List<Integer> portList = (List<Integer>) numaMap.get(NUMA_PORTS);
            numaPorts.addAll(portList);
        }
        return numaPorts;
    }

    public static List<Integer> getSlotsPorts(Map<String, Object> supervisorConf) {
        List<Integer> slotsPorts = new ArrayList<>();
        List<Number> ports = (List<Number>) supervisorConf.getOrDefault(DaemonConfig.SUPERVISOR_SLOTS_PORTS,
                new ArrayList<>());
        for (Number port : ports) {
            slotsPorts.add(port.intValue());
        }

        // It's possible we have numaPorts specified that weren't configured in SUPERVISOR_SLOTS_PORTS.  Make
        // sure we handle these ports as well.
        Set<Integer> numaPorts = SupervisorUtils.getNumaPorts(supervisorConf);
        numaPorts.removeAll(slotsPorts);
        slotsPorts.addAll(numaPorts);
        return slotsPorts;
    }

    public static void rmrAsUser(Map<String, Object> conf, String id, String path) throws IOException {
        String user = ServerUtils.getFileOwner(path);
        String logPreFix = "rmr " + id;
        List<String> commands = new ArrayList<>();
        commands.add("rmr");
        commands.add(path);
        ClientSupervisorUtils.processLauncherAndWait(conf, user, commands, null, logPreFix);
        if (Utils.checkFileExists(path)) {
            throw new RuntimeException(path + " was not deleted.");
        }
    }

    /**
     * Given the blob information returns the value of the uncompress field, handling it being a boolean value, or if
     * it's not specified then returns false.
     */
    public static boolean shouldUncompressBlob(Map<String, Object> blobInfo) {
        return ObjectReader.getBoolean(blobInfo.get("uncompress"), false);
    }

    /**
     * Given the blob information returns the value of the workerRestart field, handling it being a boolean value, or if
     * it's not specified then returns false.
     *
     * @param blobInfo the info for the blob.
     * @return true if the blob needs a worker restart by way of the callback else false.
     */
    public static boolean blobNeedsWorkerRestart(Map<String, Object> blobInfo) {
        return ObjectReader.getBoolean(blobInfo.get("workerRestart"), false);
    }

    /**
     * Returns a list of LocalResources based on the blobstore-map passed in.
     */
    public static List<LocalResource> blobstoreMapToLocalresources(Map<String, Map<String, Object>> blobstoreMap) {
        List<LocalResource> localResourceList = new ArrayList<>();
        if (blobstoreMap != null) {
            for (Map.Entry<String, Map<String, Object>> map : blobstoreMap.entrySet()) {
                Map<String, Object> blobConf = map.getValue();
                LocalResource localResource =
                    new LocalResource(map.getKey(), shouldUncompressBlob(blobConf), blobNeedsWorkerRestart(blobConf));
                localResourceList.add(localResource);
            }
        }
        return localResourceList;
    }

    public static Collection<String> supervisorWorkerIds(Map<String, Object> conf) {
        String workerRoot = ConfigUtils.workerRoot(conf);
        return ConfigUtils.readDirContents(workerRoot);
    }

    /**
     * Map from worker id to heartbeat.
     *
     */
    public static Map<String, LSWorkerHeartbeat> readWorkerHeartbeats(Map<String, Object> conf) {
        return _instance.readWorkerHeartbeatsImpl(conf);
    }

    /**
     * Get worker heartbeat by workerId.
     */
    private static LSWorkerHeartbeat readWorkerHeartbeat(Map<String, Object> conf, String workerId) {
        return _instance.readWorkerHeartbeatImpl(conf, workerId);
    }

    /**
     * Return supervisor numa configuration.
     * @param stormConf stormConf
     * @return getNumaMap
     */
    public static Map<String, Object> getNumaMap(Map<String, Object> stormConf) {
        Object numa = stormConf.get(DaemonConfig.SUPERVISOR_NUMA_META);
        if (numa == null) {
            return Collections.emptyMap();
        }
        return (Map<String, Object>) numa;
    }

    public Map<String, LSWorkerHeartbeat> readWorkerHeartbeatsImpl(Map<String, Object> conf) {
        Map<String, LSWorkerHeartbeat> workerHeartbeats = new HashMap<>();

        Collection<String> workerIds = SupervisorUtils.supervisorWorkerIds(conf);

        for (String workerId : workerIds) {
            LSWorkerHeartbeat whb = readWorkerHeartbeat(conf, workerId);
            // ATTENTION: whb can be null
            workerHeartbeats.put(workerId, whb);
        }
        return workerHeartbeats;
    }

    protected LSWorkerHeartbeat readWorkerHeartbeatImpl(Map<String, Object> conf, String workerId) {
        try {
            LocalState localState = ConfigUtils.workerState(conf, workerId);
            return localState.getWorkerHeartBeat();
        } catch (Exception e) {
            LOG.warn("Failed to read local heartbeat for workerId : {},Ignoring exception.", workerId, e);
            return null;
        }
    }
}
