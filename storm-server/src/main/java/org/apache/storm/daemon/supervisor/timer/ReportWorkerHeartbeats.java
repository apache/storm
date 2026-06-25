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
import java.util.HashMap;
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
    private final int workerMaxTimeoutSecs;

    public ReportWorkerHeartbeats(Map<String, Object> conf, Supervisor supervisor) {
        this.conf = conf;
        this.supervisor = supervisor;
        this.workerTimeoutSecs = ObjectReader.getInt(conf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS));
        this.workerMaxTimeoutSecs = ObjectReader.getInt(conf.get(Config.WORKER_MAX_TIMEOUT_SECS));
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

        // Cache the effective timeout per topology so multiple workers of the same topology on this
        // supervisor do not each re-read the topology conf within a single reporting round.
        Map<String, Integer> effectiveTimeoutCache = new HashMap<>();

        for (LSWorkerHeartbeat lsWorkerHeartbeat : localHeartbeats.values()) {
            // local worker heartbeat can be null cause some error/exception
            if (null == lsWorkerHeartbeat) {
                continue;
            }

            String topologyId = lsWorkerHeartbeat.get_topology_id();

            // Skip stale heartbeats left by worker directories that were never cleaned up
            // (e.g. a worker that died before the supervisor finished cleanup). Such a worker
            // has not heartbeat within the timeout, so it is already considered dead; forwarding
            // it would make Nimbus repeatedly read the (often deleted) topology conf and log noise.
            // A live worker always refreshes its heartbeat well within the timeout. The timeout must
            // match Slot.getHbTimeoutMs (the actual kill authority), which honors a per-topology
            // topology.worker.timeout.secs override; using only the global timeout would drop a
            // slow-but-alive worker of a longer-timeout topology one round before Slot kills it.
            int effectiveTimeoutSecs =
                effectiveTimeoutCache.computeIfAbsent(topologyId, this::effectiveWorkerTimeoutSecs);
            long hbAgeSecs = Time.deltaSecsLong(lsWorkerHeartbeat.get_time_secs());
            if (hbAgeSecs > effectiveTimeoutSecs) {
                LOG.debug("Skipping stale heartbeat for topology {}: age {}s > effective worker timeout {}s",
                    topologyId, hbAgeSecs, effectiveTimeoutSecs);
                continue;
            }

            SupervisorWorkerHeartbeat supervisorWorkerHeartbeat = new SupervisorWorkerHeartbeat();
            supervisorWorkerHeartbeat.set_storm_id(topologyId);
            supervisorWorkerHeartbeat.set_executors(lsWorkerHeartbeat.get_executors());
            supervisorWorkerHeartbeat.set_time_secs(lsWorkerHeartbeat.get_time_secs());

            heartbeatList.add(supervisorWorkerHeartbeat);
        }
        supervisorWorkerHeartbeats.set_supervisor_id(this.supervisor.getId());
        supervisorWorkerHeartbeats.set_worker_heartbeats(heartbeatList);
        return supervisorWorkerHeartbeats;
    }

    /**
     * Effective heartbeat timeout (in seconds) for a topology, agreeing with {@code Slot.getHbTimeoutMs} (the
     * actual kill authority) for the value Slot uses. A topology may raise its own timeout via
     * {@link Config#TOPOLOGY_WORKER_TIMEOUT_SECS}, which overrides the global
     * {@link Config#SUPERVISOR_WORKER_TIMEOUT_SECS} when larger.
     *
     * <p>Nimbus clamps {@link Config#TOPOLOGY_WORKER_TIMEOUT_SECS} to {@link Config#WORKER_MAX_TIMEOUT_SECS}
     * at submission time and persists the clamped value, so the on-disk conf both this method and Slot read is
     * already bounded; for that value the two computations match. Unlike Slot, this method re-applies the cap
     * to the topology override defensively, guarding against an un-clamped conf. The cap is applied to the
     * override component only (not the final result), so the global timeout is never shrunk below the value
     * Slot would use.
     *
     * <p>Orphaned worker directories often outlive their topology conf; when the conf cannot be read we fall
     * back to the global timeout, which is exactly the behavior wanted for an already-dead worker.
     */
    @VisibleForTesting
    int effectiveWorkerTimeoutSecs(String topologyId) {
        Map<String, Object> topoConf;
        try {
            topoConf = ConfigUtils.readSupervisorStormConf(conf, topologyId);
        } catch (Exception e) {
            LOG.debug("Cannot read topology conf for {}; using supervisor worker timeout {}s. msg: {}",
                topologyId, workerTimeoutSecs, e.getMessage());
            return workerTimeoutSecs;
        }
        Object topoTimeout = topoConf.get(Config.TOPOLOGY_WORKER_TIMEOUT_SECS);
        if (topoTimeout == null) {
            return workerTimeoutSecs;
        }
        int cappedTopoTimeoutSecs = Math.min(ObjectReader.getInt(topoTimeout), workerMaxTimeoutSecs);
        return Math.max(workerTimeoutSecs, cappedTopoTimeoutSecs);
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
