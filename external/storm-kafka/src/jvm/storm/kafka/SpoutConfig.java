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
 * Configuration passed to constructor of KafkaSpout.
 * 
 * As the Spout reads messages from Kafka, it maintains the KafkaSpout offsets within Zookeeper.
 *   
 * These offset values are stored under {zkRoot}/{id}.
 * 
 * @see KafkaSpout
 */
public class SpoutConfig extends KafkaConfig {
	/**
	 * List of Zookeeper host names used for Spout offset storage (without port number or other path).
	 */
    public List<String> zkServers;
	/**
	 * Zookeeper port used for Spout offset storage
	 */
    public Integer zkPort;
    /**
     * Path within Zookeeper used for Spout offset data (requires leading slash) 
     */
    public String zkRoot;
    /**
     * Identifier used within Zookeeper to identity unique Spouts.
     */
    public String id;

    // setting for how often to save the current kafka offset to ZooKeeper
    public long stateUpdateIntervalMs = 2000;

    // Exponential back-off retry settings.  These are used when retrying messages after a bolt
    // calls OutputCollector.fail().
    public long retryInitialDelayMs = 0;
    public double retryDelayMultiplier = 1.0;
    public long retryDelayMaxMs = 60 * 1000;

    /**
     * Construct configuration for KafkaSpout
     * 
     * @param hosts Hosts with Kafka topic (required)
     * @param topic Topic name (required)
     * @param zkRoot Zookeeper path for storage of Spout offsets (required)
     * @param id Identifier used to identify Spout (required)
     * 
     * @deprecated
     */
    public SpoutConfig(BrokerHosts hosts, String topic, String zkRoot, String id) {
        super(hosts, topic);
        this.zkRoot = zkRoot;
        this.id = id;
    }
    
    /**
     * Construct configuration for KafkaSpout
     * 
     * @param hosts Hosts with Kafka topic (required)
     * @param topic Topic name (required)
     * @param zkServers Zookeeper host names for Spout offset storage (required)
     * @param zkPort Zookeeper port number for Spout offset storage (required)
     * @param zkRoot Zookeeper path for storage of Spout offsets (required)
     * @param id Identifier used to identify Spout (required)
     */
    public SpoutConfig(BrokerHosts hosts, String topic, List<String> zkServers, int zkPort, String zkRoot, String id) {
        super(hosts, topic);
        this.zkServers = zkServers;
        this.zkPort = zkPort;
        this.zkRoot = zkRoot;
        this.id = id;
    }
}
