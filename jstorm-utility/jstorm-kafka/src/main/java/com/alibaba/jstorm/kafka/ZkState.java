package com.alibaba.jstorm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.ProtectACLCreateModePathAndBytesable;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkState {
    public static final Logger LOG = LoggerFactory.getLogger(ZkState.class);
    CuratorFramework _curator;

    private CuratorFramework newCurator(Map conf, KafkaSpoutConfig config) throws Exception {
        String serverPorts = "";
        List<Host> zkServers = config.zkServers;
        for (Host server : zkServers) {
            serverPorts = serverPorts + server.getHost() + ":" + server.getPort() + ",";
        }
        return CuratorFrameworkFactory.newClient(serverPorts, Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)), 15000, new RetryNTimes(
                Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)), Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
    }

    public CuratorFramework getCurator() {
        assert _curator != null;
        return _curator;
    }

    public ZkState(Map stateConf, KafkaSpoutConfig config) {
        try {
            _curator = newCurator(stateConf, config);
            _curator.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void writeJSON(String path, Map<Object, Object> data) {
        LOG.info("Writing " + path + " the data " + data.toString());
        writeBytes(path, JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8")));
    }

    public void writeBytes(String path, byte[] bytes) {
        try {
            if (_curator.checkExists().forPath(path) == null) {
                CreateBuilder builder = _curator.create();
                ProtectACLCreateModePathAndBytesable<String> createAble = (ProtectACLCreateModePathAndBytesable<String>) builder
                        .creatingParentsIfNeeded();
                createAble.withMode(CreateMode.PERSISTENT).forPath(path, bytes);
            } else {
                _curator.setData().forPath(path, bytes);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<Object, Object> readJSON(String path) {
        try {
            byte[] b = readBytes(path);
            if (b == null)
                return null;
            return (Map<Object, Object>) JSONValue.parse(new String(b, "UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] readBytes(String path) {
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

    public void close() {
        _curator.close();
        _curator = null;
    }
}