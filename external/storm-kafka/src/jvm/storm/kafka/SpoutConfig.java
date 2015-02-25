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

import java.util.List;


/**
 * Configuration settings used by KafkaSpout
 * 
 * @see KafkaSpout
 */
public class SpoutConfig extends KafkaConfig {
    public List<String> zkServers;
    public Integer zkPort;
    public String zkRoot;
    public String id;

    // setting for how often to save the current kafka offset to ZooKeeper
    public long stateUpdateIntervalMs = 2000;

    // Exponential back-off retry settings.  These are used when retrying messages after a bolt
    // calls OutputCollector.fail().
    public long retryInitialDelayMs = 0;
    public double retryDelayMultiplier = 1.0;
    public long retryDelayMaxMs = 60 * 1000;

    /**
     * Construct configuration for use with KafkaSpout
     * 
     * @param hosts Any implementation of the BrokerHosts interface, currently either ZkHosts or StaticHosts.
     * @param topic Name of the Kafka topic.
     * @param zkRoot Root directory in Zookeeper where all topics and partition information is stored. By default, this is /brokers.
     * @param id Unique identifier for this spout.
     */
    public SpoutConfig(BrokerHosts hosts, String topic, String zkRoot, String id) {
        super(hosts, topic);
        this.zkRoot = zkRoot;
        this.id = id;
    }
}
