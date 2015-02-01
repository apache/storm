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

import java.util.List;
import java.util.Map;

public class KafkaApiFactory implements KafkaFactory{

    private final List<Broker> seedBrokers;
    private final int refreshFreqSecs;

    public KafkaApiFactory(List<Broker> seedBrokers, int refreshFreqSecs) {
        this.seedBrokers = seedBrokers;
        this.refreshFreqSecs = refreshFreqSecs;
    }

    @Override
    public IBrokerReader brokerReader(Map stormConf, KafkaConfig conf) {
        return new CachedBrokerReader(new KafkaApiBrokerReader(seedBrokers, conf.topic), refreshFreqSecs);
    }

    @Override
    public PartitionCoordinator partitionCoordinator(DynamicPartitionConnections connections, ZkState state, Map conf, TopologyContext context, int totalTasks, SpoutConfig kafkaConfig, String uuid) {
        IBrokerReader brokerReader = kafkaConfig.kafkaFactory.brokerReader(conf, kafkaConfig);
        return new CachedPartitionCoordinator(connections, conf, kafkaConfig, state, context.getThisTaskIndex(), totalTasks, uuid, brokerReader, refreshFreqSecs);
    }
}
