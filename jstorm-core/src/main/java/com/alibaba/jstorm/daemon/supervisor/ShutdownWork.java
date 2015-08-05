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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.ProcessSimulator;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeUtils;

public class ShutdownWork extends RunnableCallback {

    private static Logger LOG = LoggerFactory.getLogger(ShutdownWork.class);

    /**
     * shutdown all workers
     * 
     * @param conf
     * @param supervisorId
     * @param removed
     * @param workerThreadPids
     * @param cgroupManager
     * @param block
     * @param killingWorkers
     * 
     * @return the topologys whose workers are shutdown successfully
     */
    public void shutWorker(Map conf, String supervisorId,
            Map<String, String> removed,
            ConcurrentHashMap<String, String> workerThreadPids,
            CgroupManager cgroupManager, boolean block,
            Map<String, Integer> killingWorkers,
            Map<String, Integer> taskCleanupTimeoutMap) {
        Map<String, List<String>> workerId2Pids =
                new HashMap<String, List<String>>();

        boolean localMode = false;

        int maxWaitTime = 0;

        if (killingWorkers == null)
            killingWorkers = new HashMap<String, Integer>();

        for (Entry<String, String> entry : removed.entrySet()) {
            String workerId = entry.getKey();
            String topologyId = entry.getValue();

            List<String> pids = null;
            try {
                pids = getPid(conf, workerId);
            } catch (IOException e1) {
                LOG.error("Failed to get pid for " + workerId + " of "
                        + topologyId);
            }
            workerId2Pids.put(workerId, pids);

            if (killingWorkers.get(workerId) == null) {
                killingWorkers.put(workerId, TimeUtils.current_time_secs());
                LOG.info("Begin to shut down " + topologyId + ":" + workerId);
                try {
                    String threadPid = workerThreadPids.get(workerId);

                    // local mode
                    if (threadPid != null) {
                        ProcessSimulator.killProcess(threadPid);
                        localMode = true;
                        continue;
                    }

                    for (String pid : pids) {
                        JStormUtils.process_killed(Integer.parseInt(pid));
                    }

                    if (taskCleanupTimeoutMap != null
                            && taskCleanupTimeoutMap.get(topologyId) != null) {
                        maxWaitTime =
                                Math.max(maxWaitTime,
                                        taskCleanupTimeoutMap.get(topologyId));
                    } else {
                        maxWaitTime =
                                Math.max(maxWaitTime, ConfigExtension
                                        .getTaskCleanupTimeoutSec(conf));
                    }
                } catch (Exception e) {
                    LOG.info("Failed to shutdown ", e);
                }
            }
        }

        if (block) {
            JStormUtils.sleepMs(maxWaitTime);
        }

        for (Entry<String, String> entry : removed.entrySet()) {
            String workerId = entry.getKey();
            String topologyId = entry.getValue();
            List<String> pids = workerId2Pids.get(workerId);

            int cleanupTimeout;
            if (taskCleanupTimeoutMap != null
                    && taskCleanupTimeoutMap.get(topologyId) != null) {
                cleanupTimeout = taskCleanupTimeoutMap.get(topologyId);
            } else {
                cleanupTimeout = ConfigExtension.getTaskCleanupTimeoutSec(conf);
            }

            int initCleaupTime = killingWorkers.get(workerId);
            if (TimeUtils.current_time_secs() - initCleaupTime > cleanupTimeout) {
                if (localMode == false) {
                    for (String pid : pids) {
                        JStormUtils
                                .ensure_process_killed(Integer.parseInt(pid));
                        if (cgroupManager != null) {
                            cgroupManager.shutDownWorker(workerId, true);
                        }
                    }
                }

                tryCleanupWorkerDir(conf, workerId);
                LOG.info("Successfully shut down " + workerId);
                killingWorkers.remove(workerId);
            }
        }
    }

    /**
     * clean the directory , subdirectories of STORM-LOCAL-DIR/workers/workerId
     * 
     * 
     * @param conf
     * @param workerId
     * @throws IOException
     */
    public void tryCleanupWorkerDir(Map conf, String workerId) {
        try {
            // delete heartbeat dir LOCAL_DIR/workers/workid/heartbeats
            PathUtils.rmr(StormConfig.worker_heartbeats_root(conf, workerId));
            // delete pid dir, LOCAL_DIR/workers/workerid/pids
            PathUtils.rmr(StormConfig.worker_pids_root(conf, workerId));
            // delete workerid dir, LOCAL_DIR/worker/workerid
            PathUtils.rmr(StormConfig.worker_root(conf, workerId));
        } catch (Exception e) {
            LOG.warn(e + "Failed to cleanup worker " + workerId
                    + ". Will retry later");
        }
    }

    /**
     * When worker has been started by manually and supervisor, it will return
     * multiple pid
     * 
     * @param conf
     * @param workerId
     * @return
     * @throws IOException
     */
    public List<String> getPid(Map conf, String workerId) throws IOException {
        String workerPidPath = StormConfig.worker_pids_root(conf, workerId);

        List<String> pids = PathUtils.read_dir_contents(workerPidPath);

        return pids;
    }
}
