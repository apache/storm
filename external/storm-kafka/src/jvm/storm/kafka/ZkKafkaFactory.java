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

import backtype.storm.task.TopologyContext;
import storm.kafka.trident.CachedBrokerReader;
import storm.kafka.trident.IBrokerReader;

import java.util.Map;

/**
 * @deprecated use KafkaApiFactory instead
 */
@Deprecated
public class ZkKafkaFactory implements KafkaFactory {

    private final ZkHosts hosts;

    public ZkKafkaFactory(ZkHosts hosts){
        this.hosts = hosts;
    }

    @Override
    public IBrokerReader brokerReader(Map stormConf, KafkaConfig conf) {
        return new CachedBrokerReader(new ZkBrokerReader(stormConf, hosts.brokerZkStr, hosts.brokerZkPath, conf.topic), hosts.refreshFreqSecs);
    }

    @Override
    public PartitionCoordinator partitionCoordinator(DynamicPartitionConnections connections, ZkState state, Map conf,
                                                     TopologyContext context, int totalTasks,
                                                     SpoutConfig kafkaConfig, String uuid) {
        return new CachedPartitionCoordinator(connections, conf, kafkaConfig, state, context.getThisTaskIndex(), totalTasks, uuid, brokerReader(conf, kafkaConfig), hosts.refreshFreqSecs);
    }
}
