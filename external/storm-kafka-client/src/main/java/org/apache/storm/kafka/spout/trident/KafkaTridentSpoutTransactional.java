/*
 * Copyright 2018 The Apache Software Foundation.
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

package org.apache.storm.kafka.spout.trident;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.trident.internal.OutputFieldsExtractor;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;
import org.apache.storm.tuple.Fields;

public class KafkaTridentSpoutTransactional<K, V> implements IPartitionedTridentSpout<List<Map<String, Object>>,
        KafkaTridentSpoutTopicPartition, Map<String, Object>>,
        Serializable {
    private static final long serialVersionUID = 1L;
    
    private final KafkaTridentSpoutConfig<K, V> kafkaSpoutConfig;
    private final OutputFieldsExtractor outputFieldsExtractor;

    /**
     * Creates a new non-opaque transactional Trident Kafka spout.
     */
    public KafkaTridentSpoutTransactional(KafkaTridentSpoutConfig<K, V> kafkaSpoutConfig) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;
        this.outputFieldsExtractor = new OutputFieldsExtractor();
    }
    
    @Override
    public Coordinator<List<Map<String, Object>>> getCoordinator(Map<String, Object> conf, TopologyContext context) {
        return new KafkaTridentSpoutCoordinator<>(kafkaSpoutConfig);
    }

    @Override
    public Emitter<List<Map<String, Object>>, KafkaTridentSpoutTopicPartition, Map<String, Object>> getEmitter(
        Map<String, Object> conf, TopologyContext context) {
        return new KafkaTridentTransactionalSpoutEmitter<>(new KafkaTridentSpoutEmitter<>(kafkaSpoutConfig, context));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return outputFieldsExtractor.getOutputFields(kafkaSpoutConfig);
    }
    
}
