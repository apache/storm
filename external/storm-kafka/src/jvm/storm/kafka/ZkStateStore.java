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

    public ZkStateStore(Map stateConf, SpoutConfig spoutConfig) {
        _spoutConfig = spoutConfig;

        stateConf = new HashMap(stateConf);
        try {
            _curator = newCurator(stateConf);
            _curator.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeState(Partition p, Map<Object, Object> state) {
        LOG.debug("Writing to " + committedPath(p) + " with stat data " + state.toString());
        write(committedPath(p), JSONValue.toJSONString(state).getBytes(Charset.forName("UTF-8")));
    }

    @Override
    public Map<Object, Object> readState(Partition p) {
        LOG.debug("Reading from " + committedPath(p) + " for state data");
        try {
            byte[] b = read(committedPath(p));
            if (b == null) {
                return null;
            }
            return (Map<Object, Object>) JSONValue.parse(new String(b, "UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        _curator.close();
        _curator = null;
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
