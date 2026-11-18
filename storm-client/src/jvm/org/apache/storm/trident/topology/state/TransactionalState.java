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

package org.apache.storm.trident.topology.state;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.shade.net.minidev.json.JSONValue;
import org.apache.storm.shade.net.minidev.json.parser.ParseException;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.ClientZookeeper;
import org.apache.storm.zookeeper.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that contains the logic to extract the transactional state info from zookeeper. All
 * transactional state is kept in zookeeper. This
 * class only contains references to a {@link ZkClient}, which is used to get all info from
 * zookeeper.
 */
public class TransactionalState {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionalState.class);

    ZkClient zkClient;
    List<ACL> zkAcls = null;

    protected TransactionalState(Map<String, Object> conf, String id, String subroot) {
        try {
            conf = new HashMap<>(conf);
            String transactionalRoot = (String) conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT);
            String rootDir = transactionalRoot + "/" + id + "/" + subroot;
            List<String> servers =
                    (List<String>) getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_SERVERS,
                    Config.STORM_ZOOKEEPER_SERVERS);
            Object port = getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_PORT,
                    Config.STORM_ZOOKEEPER_PORT);
            ZkClient initter = ClientZookeeper.mkClient(conf, servers, port, "", null, conf,
                    DaemonType.WORKER);
            zkAcls = Utils.getWorkerACL(conf);
            try {
                TransactionalState.createNode(initter, transactionalRoot, null, null, null);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
            }
            try {
                TransactionalState.createNode(initter, rootDir, null, zkAcls, null);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
            }
            initter.close();

            zkClient = ClientZookeeper.mkClient(conf, servers, port, rootDir, null, conf,
                    DaemonType.WORKER);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TransactionalState newUserState(Map<String, Object> conf, String id) {
        return new TransactionalState(conf, id, "user");
    }

    public static TransactionalState newCoordinatorState(Map<String, Object> conf, String id) {
        return new TransactionalState(conf, id, "coordinator");
    }

    /**
     * Create a node, and any missing parents.
     *
     * <p>{@code mode} is only honoured when {@code acls} is {@code null}; when {@code acls} is
     * given, or {@code mode} is {@code null}, the node is created {@link CreateMode#PERSISTENT}.
     * When {@code data} is {@code null} the client's default payload is stored.
     *
     * @param zk the client to use
     * @param path the path of the node
     * @param data the data to store, or {@code null} for the client's default payload
     * @param acls the ACL of the node, or {@code null} for the client's default ACL
     * @param mode the create mode, see above
     * @throws Exception on failure, ZooKeeper exceptions unwrapped
     */
    protected static void createNode(ZkClient zk, String path,
                                     byte[] data, List<ACL> acls, CreateMode mode) throws Exception {
        LOG.debug("Creating node  [path = {}],  [data = {}],  [acls = {}],  [mode = {}]", path,
                asString(data), acls, mode);

        CreateMode effectiveMode = (acls == null && mode != null) ? mode : CreateMode.PERSISTENT;
        if (data == null) {
            zk.createWithDefaultData(path, effectiveMode, acls, true);
        } else {
            zk.create(path, data, effectiveMode, acls, true);
        }
    }

    private static String asString(byte[] data) {
        return data == null ? "null" : new String(data);
    }

    public void setData(String path, Object obj) {
        path = "/" + path;
        byte[] ser;
        try {
            ser = JSONValue.toJSONString(obj).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        try {
            if (zkClient.exists(path, false) != null) {
                zkClient.setData(path, ser);
            } else {
                TransactionalState.createNode(zkClient, path, ser, zkAcls,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException nne) {
            LOG.warn("Node {} already created.", path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(String path) {
        path = "/" + path;
        try {
            zkClient.delete(path, false);
        } catch (KeeperException.NoNodeException nne) {
            LOG.warn("Path {} already deleted.", path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.debug("Deleted [path = {}]", path);
    }

    public List<String> list(String path) {
        path = "/" + path;
        try {
            List<String> children;
            if (zkClient.exists(path, false) == null) {
                children = new ArrayList<>();
            } else {
                children = zkClient.getChildren(path, false);
            }
            LOG.debug("List [path = {}], [children = {}]", path, children);
            return children;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void mkdir(String path) {
        setData(path, 7);
    }

    public Object getData(String path) {
        path = "/" + path;
        try {
            Object data;
            if (zkClient.exists(path, false) != null) {
                // Use parseWithException instead of parse so we can capture deserialization errors
                // in the log.
                // They are likely to be bugs in the spout code.
                try {
                    data = JSONValue.parseWithException(new String(zkClient.getData(path, false, null),
                            "UTF-8"));
                } catch (ParseException e) {
                    LOG.warn("Failed to deserialize zookeeper data for path {}", path, e);
                    data = null;
                }
            } else {
                data = null;
            }
            LOG.debug("Get. [path = {}] => [data = {}]", path, data);
            return data;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        zkClient.close();
    }

    private Object getWithBackup(Map<String, Object> amap, String primary, String backup) {
        Object ret = amap.get(primary);
        if (ret == null) {
            return amap.get(backup);
        }
        return ret;
    }
}
