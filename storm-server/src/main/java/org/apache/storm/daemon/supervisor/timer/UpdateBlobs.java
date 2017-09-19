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
package org.apache.storm.daemon.supervisor.timer;

import java.util.HashMap;
import org.apache.storm.Config;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.localizer.LocalResource;
import org.apache.storm.localizer.Localizer;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.NimbusLeaderNotFoundException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * downloads all blobs listed in the topology configuration for all topologies assigned to this supervisor, and creates version files with a suffix. The
 * Runnable is intended to be run periodically by a timer, created elsewhere.
 */
public class UpdateBlobs implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateBlobs.class);

    private Supervisor supervisor;

    public UpdateBlobs(Supervisor supervisor) {
        this.supervisor = supervisor;
    }

    @Override
    public void run() {
        try {
            Map<String, Object> conf = supervisor.getConf();
            Set<String> downloadedStormIds = SupervisorUtils.readDownloadedTopologyIds(conf);
            AtomicReference<Map<Long, LocalAssignment>> newAssignment = supervisor.getCurrAssignment();
            Map<String, LocalAssignment> assignedStormIds = new HashMap<>();
            for (LocalAssignment localAssignment : newAssignment.get().values()) {
                assignedStormIds.put(localAssignment.get_topology_id(), localAssignment);
            }
            for (String stormId : downloadedStormIds) {
                LocalAssignment la = assignedStormIds.get(stormId);
                if (la != null) {
                    if (la.get_owner() == null) {
                        //We got a case where the local assignment is not up to date, no point in going on...
                        LOG.warn("The blobs will not be updated for {} until the local assignment is updated...", stormId);
                    } else {
                        String stormRoot = ConfigUtils.supervisorStormDistRoot(conf, stormId);
                        LOG.debug("Checking Blob updates for storm topology id {} With target_dir: {}", stormId, stormRoot);
                        updateBlobsForTopology(conf, stormId, supervisor.getLocalizer(), la.get_owner());
                    }
                }
            }
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(TTransportException.class, e)) {
                LOG.error("Network error while updating blobs, will retry again later", e);
            } else if (Utils.exceptionCauseIsInstanceOf(NimbusLeaderNotFoundException.class, e)) {
                LOG.error("Nimbus unavailable to update blobs, will retry again later", e);
            } else {
                throw Utils.wrapInRuntime(e);
            }
        }
    }

    /**
     * Update each blob listed in the topology configuration if the latest version of the blob has not been downloaded.
     * 
     * @param conf
     * @param stormId
     * @param localizer
     * @throws IOException
     */
    private void updateBlobsForTopology(Map<String, Object> conf, String stormId, Localizer localizer, String user) throws IOException {
        Map<String, Object> topoConf = ConfigUtils.readSupervisorStormConf(conf, stormId);
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        List<LocalResource> localresources = SupervisorUtils.blobstoreMapToLocalresources(blobstoreMap);
        try {
            localizer.updateBlobs(localresources, user);
        } catch (AuthorizationException authExp) {
            LOG.error("AuthorizationException error", authExp);
        } catch (KeyNotFoundException knf) {
            LOG.error("KeyNotFoundException error", knf);
        }
    }
}
