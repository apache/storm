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
package storm.kafka.spout.helper;

import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.spout.Broker;
import storm.kafka.spout.KafkaConfig;
import storm.kafka.spout.partition.Partition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class ConsumerConnectionCache {

    public static final Logger LOG = LoggerFactory.getLogger(ConsumerConnectionCache.class);

    static class ConnectionInfo {
        SimpleConsumer consumer;
        Set<Integer> partitions = new HashSet();

        public ConnectionInfo(SimpleConsumer consumer) {
            this.consumer = consumer;
        }
    }

    private Map<Broker, ConnectionInfo> _connections = new HashMap();
    private KafkaConfig _config;

    public ConsumerConnectionCache(KafkaConfig config) {
        _config = config;
    }

    public SimpleConsumer register(Broker host, int partition) {
        if (!_connections.containsKey(host)) {
            _connections.put(host, new ConnectionInfo(new SimpleConsumer(host.host, host.port, _config.socketTimeoutMs, _config.bufferSizeBytes, _config.clientId)));
        }
        ConnectionInfo info = _connections.get(host);
        info.partitions.add(partition);
        return info.consumer;
    }

    public SimpleConsumer getConnection(Partition partition) {
        ConnectionInfo info = _connections.get(partition.host);
        if (info != null) {
            return info.consumer;
        }
        return null;
    }

    public void unregister(Broker broker, int partition) {
        ConnectionInfo info = _connections.get(broker);
        info.partitions.remove(partition);
        if (info.partitions.isEmpty()) {
            info.consumer.close();
            _connections.remove(broker);
        }
    }

    public void unregister(Partition partition) {
        unregister(partition.host, partition.partition);
    }

    public void clear() {
        for (ConnectionInfo info : _connections.values()) {
            info.consumer.close();
        }
        _connections.clear();
    }
}
