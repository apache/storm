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

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;

import java.io.Serializable;

public class KafkaConfig implements Serializable {

    /**
     * @deprecated  use kafkaFactory based construction instead
     */
    @Deprecated
    public final BrokerHosts hosts;

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
    public final KafkaFactory kafkaFactory;

    /**
     * @deprecated  use kafkaFactory based construction instead
     */
    @Deprecated
    public KafkaConfig(BrokerHosts hosts, String topic) {
        this(hosts, topic, kafka.api.OffsetRequest.DefaultClientId());
    }

    /**
     * @deprecated  use kafkaFactory based construction instead
     */
    @Deprecated
    public KafkaConfig(BrokerHosts hosts, String topic, String clientId) {
        this.hosts = hosts;
        this.topic = topic;
        this.clientId = clientId;
        if (hosts instanceof StaticHosts) {
            kafkaFactory = new StaticKafkaFactory((StaticHosts)hosts);
        } else if (hosts instanceof ZkHosts) {
            kafkaFactory = new ZkKafkaFactory((ZkHosts)hosts);
        } else {
            throw new RuntimeException("Invalid configuration for KafkaConfig.hosts. " +
                    "The use of KafkaConfig.hosts is deprecated, please use KafkaFactory");
        }
    }

    public KafkaConfig(String topic, String clientId, KafkaFactory kafkaFactory) {
        this.topic = topic;
        this.clientId = clientId;
        this.hosts = null;
        this.kafkaFactory = kafkaFactory;
    }

    public KafkaConfig(String topic, KafkaFactory kafkaFactory) {
        this(topic, kafka.api.OffsetRequest.DefaultClientId(), kafkaFactory);
    }

}
