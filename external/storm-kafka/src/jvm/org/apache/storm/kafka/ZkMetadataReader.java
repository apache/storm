/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.kafka;

import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import com.google.common.base.Preconditions;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads topics and brokers from Zookeeper
 */
public class ZkMetadataReader {

    public static final Logger LOG = LoggerFactory.getLogger(DynamicBrokersReader.class);

    private final CuratorFramework _curator;
    private final String _zkPath;

    public ZkMetadataReader(Map conf, KafkaConfig kafkaConfig) {
        // Check required parameters
        Preconditions.checkNotNull(conf, "conf cannot be null");
        Preconditions.checkNotNull(kafkaConfig, "kafkaConfig cannot be null");

        validateConfig(conf);

        ZkHosts zkHosts = (ZkHosts) kafkaConfig.hosts;
        String zkStr = zkHosts.brokerZkStr;
        _zkPath = zkHosts.brokerZkPath;

        Preconditions.checkNotNull(zkStr, "zkString cannot be null");
        Preconditions.checkNotNull(_zkPath, "zkPath cannot be null");

        try {
            _curator = CuratorFrameworkFactory.newClient(
                    zkStr,
                    Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                    Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)),
                    new RetryNTimes(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
                            Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
            _curator.start();
        } catch (Exception ex) {
            LOG.error("Couldn't connect to zookeeper", ex);
            throw new RuntimeException(ex);
        }
    }

    /**
     * @return The full list of Kafka brokers listed in Zookeeper
     */
    public List<Broker> getBrokers() throws SocketTimeoutException {
        List<Broker> brokers = new ArrayList<>();
        String brokersPath = brokerPath();
        try {
            List<String> brokerIds = _curator.getChildren().forPath(brokersPath);
            for (String brokerId : brokerIds) {
                String brokerInfoPath = brokersPath + "/" + brokerId;
                try {
                    byte[] brokerData = _curator.getData().forPath(brokerInfoPath);
                    Broker broker = getBrokerHost(brokerData);
                    brokers.add(broker);
                } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
                    //Just skip, can happen if Kafka deleted the node just now.
                    LOG.debug("Node {} does not exist ", brokerInfoPath);
                }
            }
        } catch (SocketTimeoutException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (brokers.isEmpty()) {
            throw new RuntimeException("No live brokers listed in Zookeeper at " + brokersPath);
        }
        return brokers;
    }

    public String brokerPath() {
        return _zkPath + "/ids";
    }

    /**
     * [zk: localhost:2181(CONNECTED) 56] get /brokers/ids/0 {
     * "host":"localhost", "jmx_port":9999, "port":9092, "version":1 }
     *
     * @param contents
     * @return
     */
    private Broker getBrokerHost(byte[] contents) {
        try {
            Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(contents, "UTF-8"));
            String host = (String) value.get("host");
            Integer port = ((Long) value.get("port")).intValue();
            return new Broker(host, port);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getWildcardTopics(String topicPattern) {
        List<String> topics = new ArrayList<String>();
        try {
            List<String> children = _curator.getChildren().forPath(topicsPath());
            for (String t : children) {
                if (t.matches(topicPattern)) {
                    LOG.info(String.format("Found matching topic %s", t));
                    topics.add(t);
                }
            }
            return topics;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String topicsPath() {
        return _zkPath + "/topics";
    }

    /**
     * Validate required parameters in the input configuration Map
     *
     * @param conf
     */
    private void validateConfig(final Map conf) {
        Preconditions.checkNotNull(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT),
                "%s cannot be null", Config.STORM_ZOOKEEPER_SESSION_TIMEOUT);
        Preconditions.checkNotNull(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT),
                "%s cannot be null", Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT);
        Preconditions.checkNotNull(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES),
                "%s cannot be null", Config.STORM_ZOOKEEPER_RETRY_TIMES);
        Preconditions.checkNotNull(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL),
                "%s cannot be null", Config.STORM_ZOOKEEPER_RETRY_INTERVAL);
    }

    public void close() {
        _curator.close();
    }

}
