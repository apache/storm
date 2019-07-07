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

package org.apache.storm.cluster;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.storm.callback.ZKStateChangedCallback;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.HBExecutionException;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBMessageData;
import org.apache.storm.generated.HBPulse;
import org.apache.storm.generated.HBServerMessageType;
import org.apache.storm.pacemaker.PacemakerClientPool;
import org.apache.storm.pacemaker.PacemakerConnectionException;
import org.apache.storm.shade.org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.WrappedHBExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaceMakerStateStorage implements IStateStorage {

    private static final int maxRetries = 10;
    private static Logger LOG = LoggerFactory.getLogger(PaceMakerStateStorage.class);
    private PacemakerClientPool pacemakerClientPool;
    private IStateStorage stateStorage;

    public PaceMakerStateStorage(PacemakerClientPool pacemakerClientPool, IStateStorage stateStorage) throws Exception {
        this.pacemakerClientPool = pacemakerClientPool;
        this.stateStorage = stateStorage;
    }

    @Override
    public String register(ZKStateChangedCallback callback) {
        return stateStorage.register(callback);
    }

    @Override
    public void unregister(String id) {
        stateStorage.unregister(id);
    }

    @Override
    public String create_sequential(String path, byte[] data, List<ACL> acls) {
        return stateStorage.create_sequential(path, data, acls);
    }

    @Override
    public void mkdirs(String path, List<ACL> acls) {
        stateStorage.mkdirs(path, acls);
    }

    @Override
    public void delete_node(String path) {
        stateStorage.delete_node(path);
    }

    @Override
    public void set_ephemeral_node(String path, byte[] data, List<ACL> acls) {
        stateStorage.set_ephemeral_node(path, data, acls);
    }

    @Override
    public Integer get_version(String path, boolean watch) throws Exception {
        return stateStorage.get_version(path, watch);
    }

    @Override
    public boolean node_exists(String path, boolean watch) {
        return stateStorage.node_exists(path, watch);
    }

    @Override
    public List<String> get_children(String path, boolean watch) {
        return stateStorage.get_children(path, watch);
    }

    @Override
    public void close() {
        stateStorage.close();
        pacemakerClientPool.close();
    }

    @Override
    public void set_data(String path, byte[] data, List<ACL> acls) {
        stateStorage.set_data(path, data, acls);
    }

    @Override
    public byte[] get_data(String path, boolean watch) {
        return stateStorage.get_data(path, watch);
    }

    @Override
    public VersionedData<byte[]> get_data_with_version(String path, boolean watch) {
        return stateStorage.get_data_with_version(path, watch);
    }

    @Override
    public void set_worker_hb(String path, byte[] data, List<ACL> acls) {
        int retry = maxRetries;
        while (true) {
            try {
                HBPulse hbPulse = new HBPulse();
                hbPulse.set_id(path);
                hbPulse.set_details(data);
                HBMessage message = new HBMessage(HBServerMessageType.SEND_PULSE, HBMessageData.pulse(hbPulse));
                HBMessage response = pacemakerClientPool.send(message);
                if (response.get_type() != HBServerMessageType.SEND_PULSE_RESPONSE) {
                    throw new WrappedHBExecutionException("Invalid Response Type");
                }
                LOG.debug("Successful set_worker_hb");
                break;
            } catch (HBExecutionException | PacemakerConnectionException e) {
                if (retry <= 0) {
                    throw new RuntimeException(e);
                }
                retry--;
                LOG.error("{} Failed to set_worker_hb. Will make {} more attempts.", e.getMessage(), retry);
            } catch (InterruptedException e) {
                LOG.debug("set_worker_hb got interrupted: {}", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public byte[] get_worker_hb(String path, boolean watch) {
        int retry = maxRetries;
        while (true) {
            try {
                byte[] ret = null;
                int latestTimeSecs = 0;
                boolean gotResponse = false;

                HBMessage message = new HBMessage(HBServerMessageType.GET_PULSE, HBMessageData.path(path));
                List<HBMessage> responses = pacemakerClientPool.sendAll(message);
                for (HBMessage response : responses) {
                    if (response.get_type() != HBServerMessageType.GET_PULSE_RESPONSE) {
                        LOG.error("get_worker_hb: Invalid Response Type");
                        continue;
                    }
                    // We got at least one GET_PULSE_RESPONSE message.
                    gotResponse = true;
                    byte[] details = response.get_data().get_pulse().get_details();
                    if (details == null) {
                        continue;
                    }
                    ClusterWorkerHeartbeat cwh = Utils.deserialize(details, ClusterWorkerHeartbeat.class);
                    if (cwh != null && cwh.get_time_secs() > latestTimeSecs) {
                        latestTimeSecs = cwh.get_time_secs();
                        ret = details;
                    }
                }
                if (!gotResponse) {
                    throw new WrappedHBExecutionException("Failed to get a response.");
                }
                return ret;
            } catch (HBExecutionException | PacemakerConnectionException e) {
                if (retry <= 0) {
                    throw new RuntimeException(e);
                }
                retry--;
                LOG.error("{} Failed to get_worker_hb. Will make {} more attempts.", e.getMessage(), retry);
            } catch (InterruptedException e) {
                LOG.debug("get_worker_hb got interrupted: {}", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public List<String> get_worker_hb_children(String path, boolean watch) {
        int retry = maxRetries;
        while (true) {
            try {
                HashSet<String> retSet = new HashSet<>();

                HBMessage message = new HBMessage(HBServerMessageType.GET_ALL_NODES_FOR_PATH, HBMessageData.path(path));
                List<HBMessage> responses = pacemakerClientPool.sendAll(message);
                for (HBMessage response : responses) {
                    if (response.get_type() != HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE) {
                        LOG.error("get_worker_hb_children: Invalid Response Type");
                        continue;
                    }
                    if (response.get_data().get_nodes().get_pulseIds() != null) {
                        retSet.addAll(response.get_data().get_nodes().get_pulseIds());
                    }
                }

                LOG.debug("Successful get_worker_hb_children");
                return new ArrayList<>(retSet);
            } catch (PacemakerConnectionException e) {
                if (retry <= 0) {
                    throw new RuntimeException(e);
                }
                retry--;
                LOG.error("{} Failed to get_worker_hb_children. Will make {} more attempts.", e.getMessage(), retry);
            } catch (InterruptedException e) {
                LOG.debug("get_worker_hb_children got interrupted: {}", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void delete_worker_hb(String path) {
        int retry = maxRetries;
        boolean someSucceeded;
        while (true) {
            someSucceeded = false;
            try {
                HBMessage message = new HBMessage(HBServerMessageType.DELETE_PATH, HBMessageData.path(path));
                List<HBMessage> responses = pacemakerClientPool.sendAll(message);
                boolean allSucceeded = true;
                for (HBMessage response : responses) {
                    if (response.get_type() != HBServerMessageType.DELETE_PATH_RESPONSE) {
                        LOG.debug("Failed to delete heartbeat {}", response);
                        allSucceeded = false;
                    } else {
                        someSucceeded = true;
                    }
                }
                if (allSucceeded) {
                    break;
                } else {
                    throw new WrappedHBExecutionException("Failed to delete from all pacemakers.");
                }
            } catch (HBExecutionException | PacemakerConnectionException e) {
                if (retry <= 0) {
                    if (someSucceeded) {
                        LOG.warn("Unable to delete_worker_hb from every pacemaker.");
                        break;
                    } else {
                        LOG.error("Unable to delete_worker_hb from any pacemaker.");
                        throw new RuntimeException(e);
                    }
                }
                retry--;
                LOG.debug("{} Failed to delete_worker_hb. Will make {} more attempts.", e.getMessage(), retry);
            } catch (InterruptedException e) {
                LOG.debug("delete_worker_hb got interrupted: {}", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void add_listener(ConnectionStateListener listener) {
        stateStorage.add_listener(listener);
    }

    @Override
    public void sync_path(String path) {
        stateStorage.sync_path(path);
    }

    @Override
    public void delete_node_blobstore(String path, String nimbusHostPortInfo) {
        stateStorage.delete_node_blobstore(path, nimbusHostPortInfo);
    }
}
