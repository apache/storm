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
package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZkStateStore implements StateStore {
    private static final Logger LOG = LoggerFactory.getLogger(ZkStateStore.class);

    private SpoutConfig _spoutConfig;
    private CuratorFramework _curator;

    private CuratorFramework newCurator(Map stateConf) throws Exception {
        Integer port = (Integer) stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_PORT);
        String serverPorts = "";
        for (String server : (List<String>) stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS)) {
            serverPorts = serverPorts + server + ":" + port + ",";
        }

        LOG.info("Creating new curator framework on {}.", serverPorts);
        return CuratorFrameworkFactory.newClient(serverPorts,
                Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)),
                new RetryNTimes(Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
                        Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
    }

    public CuratorFramework getCurator() {
        assert _curator != null;
        return _curator;
    }

    public ZkStateStore(Map conf, SpoutConfig spoutConfig) {
        _spoutConfig = spoutConfig;

        Map<String, Object> zkStateStoreConf = new HashMap<>(conf);
        List<String> zkServers = _spoutConfig.zkServers;
        if (zkServers == null) {
            zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        }
        Integer zkPort = _spoutConfig.zkPort;
        if (zkPort == null) {
            zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        }
        zkStateStoreConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        zkStateStoreConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        zkStateStoreConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);

        try {
            _curator = newCurator(zkStateStoreConf);
            _curator.start();
            LOG.info("Started curator framework.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeState(Partition p, Map<Object, Object> state) {
        String zkPath = committedPath(p);
        LOG.debug("Writing to {} with stat data {} for partition {}:{}.", zkPath, state, p.host, p.partition);
        write(zkPath, JSONValue.toJSONString(state).getBytes(Charset.forName("UTF-8")));
    }

    @Override
    public Map<Object, Object> readState(Partition p) {
        String zkPath = committedPath(p);
        LOG.debug("Reading from {} for state data for partition {}:{}.", zkPath, p.host, p.partition);
        try {
            byte[] b = read(zkPath);
            if (b == null) {
                LOG.warn("No state found for partition {}:{} at this time.", p.host, p.partition);
                return null;
            }
            Map<Object, Object> state = (Map<Object, Object>) JSONValue.parse(new String(b, "UTF-8"));
            LOG.debug("Retrieved state {} from {} for partition {}:{}.", state, zkPath, p.host, p.partition);
            return state;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        _curator.close();
        _curator = null;
        LOG.info("Closed curator framework.");
    }

    private String committedPath(Partition partition) {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + partition.getId();
    }

    private void write(String path, byte[] bytes) {
        try {
            if (_curator.checkExists().forPath(path) == null) {
                _curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, bytes);
            } else {
                _curator.setData().forPath(path, bytes);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] read(String path) {
        try {
            if (_curator.checkExists().forPath(path) != null) {
                return _curator.getData().forPath(path);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
