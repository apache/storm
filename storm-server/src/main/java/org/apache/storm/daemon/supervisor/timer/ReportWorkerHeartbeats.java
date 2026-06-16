/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.daemon.supervisor.timer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.apache.storm.generated.SupervisorWorkerHeartbeats;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable reporting local worker reported heartbeats to master, supervisor should take care the of the heartbeats
 * integrity for the master heartbeats recovery, a non-null node id means that the heartbeats are full,
 * and master can go on to check and wait others nodes when doing a heartbeats recovery.
 */
public class ReportWorkerHeartbeats implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ReportWorkerHeartbeats.class);

    private Supervisor supervisor;
    private Map<String, Object> conf;
    private final int workerTimeoutSecs;

    public ReportWorkerHeartbeats(Map<String, Object> conf, Supervisor supervisor) {
        this.conf = conf;
        this.supervisor = supervisor;
        this.workerTimeoutSecs = ObjectReader.getInt(conf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS));
    }

    @Override
    public void run() {
        SupervisorWorkerHeartbeats supervisorWorkerHeartbeats = getAndResetWorkerHeartbeats();
        reportWorkerHeartbeats(supervisorWorkerHeartbeats);
    }

    private SupervisorWorkerHeartbeats getAndResetWorkerHeartbeats() {
        Map<String, LSWorkerHeartbeat> localHeartbeats;
        try {
            localHeartbeats = SupervisorUtils.readWorkerHeartbeats(this.conf);
            return getSupervisorWorkerHeartbeatsFromLocal(localHeartbeats);
        } catch (Exception e) {
            LOG.error("Read local worker heartbeats error, skipping heartbeats for this round, msg:{}", e.getMessage());
            return null;
        }
    }

    @VisibleForTesting
    SupervisorWorkerHeartbeats getSupervisorWorkerHeartbeatsFromLocal(Map<String, LSWorkerHeartbeat> localHeartbeats) {
        SupervisorWorkerHeartbeats supervisorWorkerHeartbeats = new SupervisorWorkerHeartbeats();

        List<SupervisorWorkerHeartbeat> heartbeatList = new ArrayList<>();

        for (LSWorkerHeartbeat lsWorkerHeartbeat : localHeartbeats.values()) {
            // local worker heartbeat can be null cause some error/exception
            if (null == lsWorkerHeartbeat) {
                continue;
            }

            // Skip stale heartbeats left by worker directories that were never cleaned up
            // (e.g. a worker that died before the supervisor finished cleanup). Such a worker
            // has not heartbeat within the timeout, so it is already considered dead; forwarding
            // it would make Nimbus repeatedly read the (often deleted) topology conf and log noise.
            // A live worker always refreshes its heartbeat well within the timeout.
            long hbAgeSecs = Time.deltaSecsLong(lsWorkerHeartbeat.get_time_secs());
            if (hbAgeSecs > workerTimeoutSecs) {
                LOG.debug("Skipping stale heartbeat for topology {}: age {}s > worker timeout {}s",
                    lsWorkerHeartbeat.get_topology_id(), hbAgeSecs, workerTimeoutSecs);
                continue;
            }

            SupervisorWorkerHeartbeat supervisorWorkerHeartbeat = new SupervisorWorkerHeartbeat();
            supervisorWorkerHeartbeat.set_storm_id(lsWorkerHeartbeat.get_topology_id());
            supervisorWorkerHeartbeat.set_executors(lsWorkerHeartbeat.get_executors());
            supervisorWorkerHeartbeat.set_time_secs(lsWorkerHeartbeat.get_time_secs());

            heartbeatList.add(supervisorWorkerHeartbeat);
        }
        supervisorWorkerHeartbeats.set_supervisor_id(this.supervisor.getId());
        supervisorWorkerHeartbeats.set_worker_heartbeats(heartbeatList);
        return supervisorWorkerHeartbeats;
    }

    private void reportWorkerHeartbeats(SupervisorWorkerHeartbeats supervisorWorkerHeartbeats) {
        if (supervisorWorkerHeartbeats == null) {
            // error/exception thrown, just skip
            return;
        }
        if (supervisor.getStormClusterState().isPacemakerStateStore()) {
            LOG.debug("Worker are using pacemaker to send worker heartbeats so skip reporting by supervisor.");
            return;
        }
        // if it is local mode, just get the local nimbus instance and set the heartbeats
        if (ConfigUtils.isLocalMode(conf)) {
            try {
                this.supervisor.getLocalNimbus().sendSupervisorWorkerHeartbeats(supervisorWorkerHeartbeats);
            } catch (TException tex) {
                LOG.error("Send local supervisor heartbeats error", tex);
            }
        } else {
            try (NimbusClient master = NimbusClient.Builder.withConf(conf).forDaemon().build()) {
                master.getClient().sendSupervisorWorkerHeartbeats(supervisorWorkerHeartbeats);
            } catch (Exception t) {
                LOG.error("Send worker heartbeats to master exception", t);
            }
        }
    }
}
