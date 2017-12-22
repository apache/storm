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

package org.apache.storm.kafka.trident;

import java.util.Map;
import java.util.Properties;
import org.apache.storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.KafkaTopicSelector;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TridentKafkaStateFactory<K, V> implements StateFactory {

    private static final long serialVersionUID = -3613240970062343385L;
    private static final Logger LOG = LoggerFactory.getLogger(TridentKafkaStateFactory.class);

    private TridentTupleToKafkaMapper<K, V> mapper;
    private KafkaTopicSelector topicSelector;
    private Properties producerProperties = new Properties();

    public TridentKafkaStateFactory<K, V> withTridentTupleToKafkaMapper(TridentTupleToKafkaMapper<K, V> mapper) {
        this.mapper = mapper;
        return this;
    }

    public TridentKafkaStateFactory<K, V> withKafkaTopicSelector(KafkaTopicSelector selector) {
        this.topicSelector = selector;
        return this;
    }

    public TridentKafkaStateFactory<K, V> withProducerProperties(Properties props) {
        this.producerProperties = props;
        return this;
    }

    @Override
    public State makeState(Map<String, Object> conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("makeState(partitonIndex={}, numpartitions={}", partitionIndex, numPartitions);
        TridentKafkaState<K, V> state = new TridentKafkaState<>();
        state.withKafkaTopicSelector(this.topicSelector)
            .withTridentTupleToKafkaMapper(this.mapper);
        state.prepare(producerProperties);
        return state;
    }
}
