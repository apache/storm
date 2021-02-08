/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout.trident;

import java.util.List;
import java.util.Map;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.kafka.spout.trident.internal.OutputFieldsExtractor;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTridentSpoutOpaque<K, V> implements IOpaquePartitionedTridentSpout<List<Map<String, Object>>,
        KafkaTridentSpoutTopicPartition, Map<String, Object>> {
    private static final long serialVersionUID = -8003272486566259640L;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTridentSpoutOpaque.class);

    private final KafkaTridentSpoutConfig<K, V> kafkaSpoutConfig;
    private final OutputFieldsExtractor outputFieldsExtractor;
    
    /**
     * Creates a new opaque transactional Trident Kafka spout.
     */
    public KafkaTridentSpoutOpaque(KafkaTridentSpoutConfig<K, V> kafkaSpoutConfig) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;
        this.outputFieldsExtractor = new OutputFieldsExtractor();
        LOG.debug("Created {}", this.toString());
    }

    @Override
    public Emitter<List<Map<String, Object>>, KafkaTridentSpoutTopicPartition, Map<String, Object>> getEmitter(
            Map<String, Object> conf, TopologyContext context) {
        return new KafkaTridentOpaqueSpoutEmitter<>(new KafkaTridentSpoutEmitter<>(kafkaSpoutConfig, context));
    }

    @Override
    public Coordinator<List<Map<String, Object>>> getCoordinator(Map<String, Object> conf, TopologyContext context) {
        return new KafkaTridentSpoutCoordinator<>(kafkaSpoutConfig);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        final Fields outputFields = outputFieldsExtractor.getOutputFields(kafkaSpoutConfig);
        LOG.debug("OutputFields = {}", outputFields);
        return outputFields;
    }

    @Override
    public final String toString() {
        return super.toString()
                + "{kafkaSpoutConfig=" + kafkaSpoutConfig + '}';
    }
}
