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
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.api.PathAndBytesable;
import org.apache.storm.shade.org.apache.curator.framework.api.ProtectACLCreateModePathAndBytesable;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.utils.CuratorUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ZookeeperAuthInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that contains the logic to extract the transactional state info from zookeeper. All transactional state is kept in zookeeper. This
 * class only contains references to Curator, which is used to get all info from zookeeper.
 */
public class TransactionalState {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionalState.class);

    CuratorFramework curator;
    List<ACL> zkAcls = null;

    protected TransactionalState(Map<String, Object> conf, String id, String subroot) {
        try {
            conf = new HashMap<>(conf);
            String transactionalRoot = (String) conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT);
            String rootDir = transactionalRoot + "/" + id + "/" + subroot;
            List<String> servers =
                (List<String>) getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, Config.STORM_ZOOKEEPER_SERVERS);
            Object port = getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_PORT, Config.STORM_ZOOKEEPER_PORT);
            ZookeeperAuthInfo auth = new ZookeeperAuthInfo(conf);
            CuratorFramework initter = CuratorUtils.newCuratorStarted(conf, servers, port, auth, DaemonType.WORKER.getDefaultZkAcls(conf));
            zkAcls = Utils.getWorkerACL(conf);
            try {
                TransactionalState.createNode(initter, transactionalRoot, null, null, null);
            } catch (KeeperException.NodeExistsException e) {
                //ignore
            }
            try {
                TransactionalState.createNode(initter, rootDir, null, zkAcls, null);
            } catch (KeeperException.NodeExistsException e) {
                //ignore
            }
            initter.close();

            curator = CuratorUtils.newCuratorStarted(conf, servers, port, rootDir, auth, DaemonType.WORKER.getDefaultZkAcls(conf));
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

    protected static String forPath(PathAndBytesable<String> builder,
                                    String path, byte[] data) throws Exception {
        return (data == null)
            ? builder.forPath(path)
            : builder.forPath(path, data);
    }

    protected static void createNode(CuratorFramework curator, String path,
                                     byte[] data, List<ACL> acls, CreateMode mode) throws Exception {
        ProtectACLCreateModePathAndBytesable<String> builder =
            curator.create().creatingParentsIfNeeded();
        LOG.debug("Creating node  [path = {}],  [data = {}],  [acls = {}],  [mode = {}]", path, asString(data), acls, mode);

        if (acls == null) {
            if (mode == null) {
                TransactionalState.forPath(builder, path, data);
            } else {
                TransactionalState.forPath(builder.withMode(mode), path, data);
            }
            return;
        }

        TransactionalState.forPath(builder.withACL(acls), path, data);
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
            if (curator.checkExists().forPath(path) != null) {
                curator.setData().forPath(path, ser);
            } else {
                TransactionalState.createNode(curator, path, ser, zkAcls,
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
            curator.delete().forPath(path);
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
            if (curator.checkExists().forPath(path) == null) {
                children = new ArrayList<>();
            } else {
                children = curator.getChildren().forPath(path);
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
            if (curator.checkExists().forPath(path) != null) {
                // Use parseWithException instead of parse so we can capture deserialization errors in the log.
                // They are likely to be bugs in the spout code.
                try {
                    data = JSONValue.parseWithException(new String(curator.getData().forPath(path), "UTF-8"));
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
        curator.close();
    }

    private Object getWithBackup(Map<String, Object> amap, String primary, String backup) {
        Object ret = amap.get(primary);
        if (ret == null) {
            return amap.get(backup);
        }
        return ret;
    }
}
