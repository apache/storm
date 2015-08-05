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
package com.alibaba.jstorm.daemon.supervisor;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.LocalState;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.LocalAssignment;
import com.alibaba.jstorm.event.EventManager;
import com.alibaba.jstorm.event.EventManagerZkPusher;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * supervisor SynchronizeSupervisor workflow (1) writer local assignment to
 * LocalState (2) download new Assignment's topology (3) remove useless Topology
 * (4) push one SyncProcessEvent to SyncProcessEvent's EventManager
 */
class SyncSupervisorEvent extends RunnableCallback {

    private static final Logger LOG = LoggerFactory
            .getLogger(SyncSupervisorEvent.class);

    // private Supervisor supervisor;

    private String supervisorId;

    private EventManager processEventManager;

    private EventManager syncSupEventManager;

    private StormClusterState stormClusterState;

    private LocalState localState;

    private Map<Object, Object> conf;

    private SyncProcessEvent syncProcesses;

    private int lastTime;

    private Heartbeat heartbeat;

    /**
     * @param conf
     * @param processEventManager
     * @param syncSupEventManager
     * @param stormClusterState
     * @param supervisorId
     * @param localState
     * @param syncProcesses
     */
    public SyncSupervisorEvent(String supervisorId, Map conf,
            EventManager processEventManager, EventManager syncSupEventManager,
            StormClusterState stormClusterState, LocalState localState,
            SyncProcessEvent syncProcesses, Heartbeat heartbeat) {

        this.syncProcesses = syncProcesses;
        this.processEventManager = processEventManager;
        this.syncSupEventManager = syncSupEventManager;
        this.stormClusterState = stormClusterState;
        this.conf = conf;
        this.supervisorId = supervisorId;
        this.localState = localState;
        this.heartbeat = heartbeat;
    }

    @Override
    public void run() {
        LOG.debug("Synchronizing supervisor, interval seconds:"
                + TimeUtils.time_delta(lastTime));
        lastTime = TimeUtils.current_time_secs();

        try {

            RunnableCallback syncCallback =
                    new EventManagerZkPusher(this, syncSupEventManager);

            /**
             * Step 1: get all assignments and register /ZK-dir/assignment and
             * every assignment watch
             * 
             */
            Map<String, Assignment> assignments =
                    Cluster.get_all_assignment(stormClusterState, syncCallback);
            LOG.debug("Get all assignments " + assignments);

            /**
             * Step 2: get topologyIds list from
             * STORM-LOCAL-DIR/supervisor/stormdist/
             */
            List<String> downloadedTopologyIds =
                    StormConfig.get_supervisor_toplogy_list(conf);
            LOG.debug("Downloaded storm ids: " + downloadedTopologyIds);

            /**
             * Step 3: get <port,LocalAssignments> from ZK local node's
             * assignment
             */
            Map<Integer, LocalAssignment> zkAssignment =
                    getLocalAssign(stormClusterState, supervisorId, assignments);
            Map<Integer, LocalAssignment> localAssignment;
            Set<String> updateTopologys;

            /**
             * Step 4: writer local assignment to LocalState
             */
            try {
                LOG.debug("Writing local assignment " + zkAssignment);
                localAssignment =
                        (Map<Integer, LocalAssignment>) localState
                                .get(Common.LS_LOCAL_ASSIGNMENTS);
                if (localAssignment == null) {
                    localAssignment = new HashMap<Integer, LocalAssignment>();
                }
                localState.put(Common.LS_LOCAL_ASSIGNMENTS, zkAssignment);

                updateTopologys =
                        getUpdateTopologys(localAssignment, zkAssignment);
                Set<String> reDownloadTopologys =
                        getNeedReDownloadTopologys(localAssignment);
                if (reDownloadTopologys != null) {
                    updateTopologys.addAll(reDownloadTopologys);
                }
            } catch (IOException e) {
                LOG.error("put LS_LOCAL_ASSIGNMENTS " + zkAssignment
                        + " of localState failed");
                throw e;
            }

            /**
             * Step 5: download code from ZK
             */
            Map<String, String> topologyCodes =
                    getTopologyCodeLocations(assignments, supervisorId);

            downloadTopology(topologyCodes, downloadedTopologyIds,
                    updateTopologys, assignments);

            /**
             * Step 6: remove any downloaded useless topology
             */
            removeUselessTopology(topologyCodes, downloadedTopologyIds);

            /**
             * Step 7: push syncProcesses Event
             */
            // processEventManager.add(syncProcesses);
            syncProcesses.run(zkAssignment);

            // If everything is OK, set the trigger to update heartbeat of
            // supervisor
            heartbeat.updateHbTrigger(true);
        } catch (Exception e) {
            LOG.error("Failed to Sync Supervisor", e);
            // throw new RuntimeException(e);
        }

    }

    /**
     * download code ; two cluster mode: local and distributed
     * 
     * @param conf
     * @param topologyId
     * @param masterCodeDir
     * @param clusterMode
     * @throws IOException
     */
    private void downloadStormCode(Map conf, String topologyId,
            String masterCodeDir) throws IOException, TException {
        String clusterMode = StormConfig.cluster_mode(conf);

        if (clusterMode.endsWith("distributed")) {
            downloadDistributeStormCode(conf, topologyId, masterCodeDir);
        } else if (clusterMode.endsWith("local")) {
            downloadLocalStormCode(conf, topologyId, masterCodeDir);

        }
    }

    private void downloadLocalStormCode(Map conf, String topologyId,
            String masterCodeDir) throws IOException, TException {

        // STORM-LOCAL-DIR/supervisor/stormdist/storm-id
        String stormroot =
                StormConfig.supervisor_stormdist_root(conf, topologyId);

        FileUtils.copyDirectory(new File(masterCodeDir), new File(stormroot));

        ClassLoader classloader =
                Thread.currentThread().getContextClassLoader();

        String resourcesJar = resourcesJar();

        URL url = classloader.getResource(StormConfig.RESOURCES_SUBDIR);

        String targetDir = stormroot + '/' + StormConfig.RESOURCES_SUBDIR;

        if (resourcesJar != null) {

            LOG.info("Extracting resources from jar at " + resourcesJar
                    + " to " + targetDir);

            JStormUtils.extract_dir_from_jar(resourcesJar,
                    StormConfig.RESOURCES_SUBDIR, stormroot);// extract dir
            // from jar;;
            // util.clj
        } else if (url != null) {

            LOG.info("Copying resources at " + url.toString() + " to "
                    + targetDir);

            FileUtils.copyDirectory(new File(url.getFile()), (new File(
                    targetDir)));

        }
    }

    /**
     * Don't need synchronize, due to EventManager will execute serially
     * 
     * @param conf
     * @param topologyId
     * @param masterCodeDir
     * @throws IOException
     * @throws TException
     */
    private void downloadDistributeStormCode(Map conf, String topologyId,
            String masterCodeDir) throws IOException, TException {

        // STORM_LOCAL_DIR/supervisor/tmp/(UUID)
        String tmproot =
                StormConfig.supervisorTmpDir(conf) + File.separator
                        + UUID.randomUUID().toString();

        // STORM_LOCAL_DIR/supervisor/stormdist/topologyId
        String stormroot =
                StormConfig.supervisor_stormdist_root(conf, topologyId);

        JStormServerUtils.downloadCodeFromMaster(conf, tmproot, masterCodeDir,
                topologyId, true);

        // tmproot/stormjar.jar
        String localFileJarTmp = StormConfig.stormjar_path(tmproot);

        // extract dir from jar
        JStormUtils.extract_dir_from_jar(localFileJarTmp,
                StormConfig.RESOURCES_SUBDIR, tmproot);

        File srcDir = new File(tmproot);
        File destDir = new File(stormroot);
        try {
            FileUtils.moveDirectory(srcDir, destDir);
        } catch (FileExistsException e) {
            FileUtils.copyDirectory(srcDir, destDir);
            FileUtils.deleteQuietly(srcDir);
        }
    }

    private String resourcesJar() {

        String path = System.getProperty("java.class.path");
        if (path == null) {
            return null;
        }

        String[] paths = path.split(File.pathSeparator);

        List<String> jarPaths = new ArrayList<String>();
        for (String s : paths) {
            if (s.endsWith(".jar")) {
                jarPaths.add(s);
            }
        }

        /**
         * FIXME, this place seems exist problem
         */
        List<String> rtn = new ArrayList<String>();
        int size = jarPaths.size();
        for (int i = 0; i < size; i++) {
            if (JStormUtils.zipContainsDir(jarPaths.get(i),
                    StormConfig.RESOURCES_SUBDIR)) {
                rtn.add(jarPaths.get(i));
            }
        }

        if (rtn.size() == 0)
            return null;

        return rtn.get(0);
    }

    /**
     * a port must be assigned one topology
     * 
     * @param stormClusterState
     * @param supervisorId
     * @param callback
     * @throws Exception
     * @returns map: {port,LocalAssignment}
     */
    private Map<Integer, LocalAssignment> getLocalAssign(
            StormClusterState stormClusterState, String supervisorId,
            Map<String, Assignment> assignments) throws Exception {

        Map<Integer, LocalAssignment> portLA =
                new HashMap<Integer, LocalAssignment>();

        for (Entry<String, Assignment> assignEntry : assignments.entrySet()) {
            String topologyId = assignEntry.getKey();
            Assignment assignment = assignEntry.getValue();

            Map<Integer, LocalAssignment> portTasks =
                    readMyTasks(stormClusterState, topologyId, supervisorId,
                            assignment);
            if (portTasks == null) {
                continue;
            }

            // a port must be assigned one storm
            for (Entry<Integer, LocalAssignment> entry : portTasks.entrySet()) {

                Integer port = entry.getKey();

                LocalAssignment la = entry.getValue();

                if (!portLA.containsKey(port)) {
                    portLA.put(port, la);
                } else {
                    throw new RuntimeException(
                            "Should not have multiple topologys assigned to one port");
                }
            }
        }

        return portLA;
    }

    /**
     * get local node's tasks
     * 
     * @param stormClusterState
     * @param topologyId
     * @param supervisorId
     * @param callback
     * @return Map: {port, LocalAssignment}
     * @throws Exception
     */
    private Map<Integer, LocalAssignment> readMyTasks(
            StormClusterState stormClusterState, String topologyId,
            String supervisorId, Assignment assignmenInfo) throws Exception {

        Map<Integer, LocalAssignment> portTasks =
                new HashMap<Integer, LocalAssignment>();

        Set<ResourceWorkerSlot> workers = assignmenInfo.getWorkers();
        if (workers == null) {
            LOG.error("No worker of assignement's " + assignmenInfo);
            return portTasks;
        }

        for (ResourceWorkerSlot worker : workers) {
            if (!supervisorId.equals(worker.getNodeId()))
                continue;
            portTasks.put(worker.getPort(), new LocalAssignment(topologyId,
                    worker.getTasks(), Common.topologyIdToName(topologyId),
                    worker.getMemSize(), worker.getCpu(), worker.getJvm(),
                    assignmenInfo.getTimeStamp()));
        }

        return portTasks;
    }

    /**
     * get mastercodedir for every topology
     * 
     * @param stormClusterState
     * @param callback
     * @throws Exception
     * @returns Map: <topologyId, master-code-dir> from zookeeper
     */
    public static Map<String, String> getTopologyCodeLocations(
            Map<String, Assignment> assignments, String supervisorId)
            throws Exception {

        Map<String, String> rtn = new HashMap<String, String>();
        for (Entry<String, Assignment> entry : assignments.entrySet()) {
            String topologyid = entry.getKey();
            Assignment assignmenInfo = entry.getValue();

            Set<ResourceWorkerSlot> workers = assignmenInfo.getWorkers();
            for (ResourceWorkerSlot worker : workers) {
                String node = worker.getNodeId();
                if (supervisorId.equals(node)) {
                    rtn.put(topologyid, assignmenInfo.getMasterCodeDir());
                    break;
                }
            }

        }
        return rtn;
    }

    public void downloadTopology(Map<String, String> topologyCodes,
            List<String> downloadedTopologyIds, Set<String> updateTopologys,
            Map<String, Assignment> assignments) throws Exception {

        Set<String> downloadTopologys = new HashSet<String>();

        for (Entry<String, String> entry : topologyCodes.entrySet()) {

            String topologyId = entry.getKey();
            String masterCodeDir = entry.getValue();

            if (!downloadedTopologyIds.contains(topologyId)
                    || updateTopologys.contains(topologyId)) {

                LOG.info("Downloading code for storm id " + topologyId
                        + " from " + masterCodeDir);

                try {
                    downloadStormCode(conf, topologyId, masterCodeDir);
                    // Update assignment timeStamp
                    StormConfig.write_supervisor_topology_timestamp(conf,
                            topologyId, assignments.get(topologyId)
                                    .getTimeStamp());
                } catch (IOException e) {
                    LOG.error(e + " downloadStormCode failed " + "topologyId:"
                            + topologyId + "masterCodeDir:" + masterCodeDir);

                } catch (TException e) {
                    LOG.error(e + " downloadStormCode failed " + "topologyId:"
                            + topologyId + "masterCodeDir:" + masterCodeDir);
                }
                LOG.info("Finished downloading code for storm id " + topologyId
                        + " from " + masterCodeDir);

                downloadTopologys.add(topologyId);
            }
        }

        updateTaskCleanupTimeout(downloadTopologys);
    }

    public void removeUselessTopology(Map<String, String> topologyCodes,
            List<String> downloadedTopologyIds) {
        for (String topologyId : downloadedTopologyIds) {

            if (!topologyCodes.containsKey(topologyId)) {

                LOG.info("Removing code for storm id " + topologyId);

                String path = null;
                try {
                    path =
                            StormConfig.supervisor_stormdist_root(conf,
                                    topologyId);
                    PathUtils.rmr(path);
                } catch (IOException e) {
                    String errMsg = "rmr the path:" + path + "failed\n";
                    LOG.error(errMsg, e);
                }
            }
        }
    }

    private Set<String> getUpdateTopologys(
            Map<Integer, LocalAssignment> localAssignments,
            Map<Integer, LocalAssignment> zkAssignments) {
        Set<String> ret = new HashSet<String>();
        if (localAssignments != null && zkAssignments != null) {
            for (Entry<Integer, LocalAssignment> entry : localAssignments
                    .entrySet()) {
                Integer port = entry.getKey();
                LocalAssignment localAssignment = entry.getValue();

                LocalAssignment zkAssignment = zkAssignments.get(port);

                if (localAssignment == null || zkAssignment == null)
                    continue;

                if (localAssignment.getTopologyId().equals(
                        zkAssignment.getTopologyId())
                        && localAssignment.getTimeStamp() < zkAssignment
                                .getTimeStamp())
                    if (ret.add(localAssignment.getTopologyId())) {
                        LOG.info("Topology-" + localAssignment.getTopologyId()
                                + " has been updated. LocalTs="
                                + localAssignment.getTimeStamp() + ", ZkTs="
                                + zkAssignment.getTimeStamp());
                    }
            }
        }

        return ret;
    }

    private Set<String> getNeedReDownloadTopologys(
            Map<Integer, LocalAssignment> localAssignment) {
        Set<String> reDownloadTopologys =
                syncProcesses.getTopologyIdNeedDownload().getAndSet(null);
        if (reDownloadTopologys == null || reDownloadTopologys.size() == 0)
            return null;
        Set<String> needRemoveTopologys = new HashSet<String>();
        Map<Integer, String> portToStartWorkerId =
                syncProcesses.getPortToWorkerId();
        for (Entry<Integer, LocalAssignment> entry : localAssignment
                .entrySet()) {
            if (portToStartWorkerId.containsKey(entry.getKey()))
                needRemoveTopologys.add(entry.getValue().getTopologyId());
        }
        LOG.debug(
                "worker is starting on these topology, so delay download topology binary: "
                        + needRemoveTopologys);
        reDownloadTopologys.removeAll(needRemoveTopologys);
        if (reDownloadTopologys.size() > 0)
            LOG.info("Following topologys is going to re-download the jars, "
                    + reDownloadTopologys);
        return reDownloadTopologys;
    }

    private void updateTaskCleanupTimeout(Set<String> topologys) {
        Map topologyConf = null;
        Map<String, Integer> taskCleanupTimeouts =
                new HashMap<String, Integer>();

        for (String topologyId : topologys) {
            try {
                topologyConf =
                        StormConfig.read_supervisor_topology_conf(conf,
                                topologyId);
            } catch (IOException e) {
                LOG.info("Failed to read conf for " + topologyId);
            }

            Integer cleanupTimeout = null;
            if (topologyConf != null) {
                cleanupTimeout =
                        JStormUtils.parseInt(topologyConf
                                .get(ConfigExtension.TASK_CLEANUP_TIMEOUT_SEC));
            }

            if (cleanupTimeout == null) {
                cleanupTimeout = ConfigExtension.getTaskCleanupTimeoutSec(conf);
            }

            taskCleanupTimeouts.put(topologyId, cleanupTimeout);
        }

        Map<String, Integer> localTaskCleanupTimeouts = null;
        try {
            localTaskCleanupTimeouts =
                    (Map<String, Integer>) localState
                            .get(Common.LS_TASK_CLEANUP_TIMEOUT);
        } catch (IOException e) {
            LOG.error("Failed to read local task cleanup timeout map", e);
        }

        if (localTaskCleanupTimeouts == null)
            localTaskCleanupTimeouts = taskCleanupTimeouts;
        else
            localTaskCleanupTimeouts.putAll(taskCleanupTimeouts);

        try {
            localState.put(Common.LS_TASK_CLEANUP_TIMEOUT,
                    localTaskCleanupTimeouts);
        } catch (IOException e) {
            LOG.error("Failed to write local task cleanup timeout map", e);
        }
    }
}
