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
package storm.kafka.spout;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;

import java.io.Serializable;
import java.util.List;

public class KafkaConfig implements Serializable {

    public final String topic;
    public final String clientId;

    public int fetchSizeBytes = 1024 * 1024;
    public int socketTimeoutMs = 10000;
    public int fetchMaxWait = 10000;
    public int bufferSizeBytes = 1024 * 1024;
    public MultiScheme scheme = new RawMultiScheme();
    public boolean forceFromStart = false;
    public long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
    public long maxOffsetBehind = Long.MAX_VALUE;
    public boolean useStartOffsetTimeIfOffsetOutOfRange = true;
    public int metricsTimeBucketSizeInSecs = 60;

    public List<Broker> seedBrokers;

    // setting for how often to save the current kafka offset to ZooKeeper
    public long stateUpdateIntervalMs = 2000;

    // Exponential back-off retry settings.  These are used when retrying messages after a bolt
    // calls OutputCollector.fail().
    public long retryInitialDelayMs = 0;
    public double retryDelayMultiplier = 1.0;
    public long retryDelayMaxMs = 60 * 1000;

    public KafkaConfig(List<Broker> seedBrokers, String topic) {
        this(seedBrokers, topic, kafka.api.OffsetRequest.DefaultClientId());
    }

    public KafkaConfig(List<Broker> seedBrokers, String topic, String clientId) {
        this.seedBrokers = seedBrokers;
        this.topic = topic;
        this.clientId = clientId;
    }

}
