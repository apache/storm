/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.sql.kafka;

import com.google.common.base.Preconditions;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

import org.apache.storm.spout.Scheme;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.IOutputSerializer;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.sql.runtime.utils.SerdeUtils;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Create a Kafka spout/sink based on the URI and properties. The URI has the format of
 * kafka://topic?bootstrap-servers=ip:port[,ip:port]. The properties are in JSON format which specifies the producer config
 * of the Kafka broker.
 */
public class KafkaDataSourcesProvider implements DataSourcesProvider {
    private static final String CONFIG_KEY_PRODUCER = "producer";
    private static final String URI_PARAMS_BOOTSTRAP_SERVERS = "bootstrap-servers";

    private static class SqlKafkaMapper implements TupleToKafkaMapper<Object, ByteBuffer> {
        private final IOutputSerializer serializer;

        private SqlKafkaMapper(IOutputSerializer serializer) {
            this.serializer = serializer;
        }

        @Override
        public Object getKeyFromTuple(Tuple tuple) {
            return tuple.getValue(0);
        }

        @Override
        public ByteBuffer getMessageFromTuple(Tuple tuple) {
            return serializer.write((Values) tuple.getValue(1), null);
        }
    }

    private static class KafkaStreamsDataSource implements ISqlStreamsDataSource {
        private final KafkaSpoutConfig<ByteBuffer, ByteBuffer> kafkaSpoutConfig;
        private final String bootstrapServers;
        private final String topic;
        private final Properties props;
        private final IOutputSerializer serializer;

        KafkaStreamsDataSource(KafkaSpoutConfig<ByteBuffer, ByteBuffer> kafkaSpoutConfig, String bootstrapServers,
            String topic, Properties props, IOutputSerializer serializer) {
            this.kafkaSpoutConfig = kafkaSpoutConfig;
            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
            this.props = props;
            this.serializer = serializer;
        }

        @Override
        public IRichSpout getProducer() {
            return new KafkaSpout<>(kafkaSpoutConfig);
        }

        @Override
        public IRichBolt getConsumer() {
            Preconditions.checkArgument(!props.isEmpty(),
                    "Writable Kafka table " + topic + " must contain producer config");
            HashMap<String, Object> producerConfig = (HashMap<String, Object>) props.get(CONFIG_KEY_PRODUCER);
            props.putAll(producerConfig);
            Preconditions.checkState(!props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                    "Writable Kafka table " + topic + " must not contain \"bootstrap.servers\" config, set it in the kafka URL instead");
            Preconditions.checkState(!props.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG),
                "Writable Kafka table " + topic + "must not contain " + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
                    + ", it will be hardcoded to be " + ByteBufferSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class);
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            TupleToKafkaMapper<Object, ByteBuffer> mapper = new SqlKafkaMapper(serializer);

            return new KafkaBolt<Object, ByteBuffer>()
                    .withTopicSelector(new DefaultTopicSelector(topic))
                    .withProducerProperties(props)
                    .withTupleToKafkaMapper(mapper);
        }
    }

    @Override
    public String scheme() {
        return "kafka";
    }
    
    @Override
    public ISqlStreamsDataSource constructStreams(URI uri, String inputFormatClass, String outputFormatClass,
                                                  Properties properties, List<FieldInfo> fields) {
        List<String> fieldNames = new ArrayList<>();
        int primaryIndex = -1;
        for (int i = 0; i < fields.size(); ++i) {
            FieldInfo f = fields.get(i);
            fieldNames.add(f.name());
            if (f.isPrimary()) {
                primaryIndex = i;
            }
        }
        Preconditions.checkState(primaryIndex != -1, "Kafka stream table must have a primary key");
        Scheme scheme = SerdeUtils.getScheme(inputFormatClass, properties, fieldNames);
        
        Map<String, String> values = parseUriParams(uri.getQuery());
        String bootstrapServers = values.get(URI_PARAMS_BOOTSTRAP_SERVERS);
        Preconditions.checkNotNull(bootstrapServers, "bootstrap-servers must be specified");
        String topic = uri.getHost();
        KafkaSpoutConfig<ByteBuffer, ByteBuffer> kafkaSpoutConfig = 
            new KafkaSpoutConfig.Builder<ByteBuffer, ByteBuffer>(bootstrapServers, topic)
            .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class)
            .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, "storm-sql-kafka-" + UUID.randomUUID().toString())
            .setRecordTranslator(new RecordTranslatorSchemeAdapter(scheme))
            .build();

        IOutputSerializer serializer = SerdeUtils.getSerializer(outputFormatClass, properties, fieldNames);

        return new KafkaStreamsDataSource(kafkaSpoutConfig, bootstrapServers, topic, properties, serializer);
    }

    private static Map<String, String> parseUriParams(String query) {
        HashMap<String, String> res = new HashMap<>();
        if (query == null) {
            return res;
        }

        String[] params = query.split("&");
        for (String p : params) {
            String[] v = p.split("=", 2);
            if (v.length > 1) {
                res.put(v[0], v[1]);
            }
        }
        return res;
    }
}
