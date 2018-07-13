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

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
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
 * kafka://zkhost:port/broker_path?topic=topic. The properties are in JSON format which specifies the producer config
 * of the Kafka broker.
 */
public class KafkaDataSourcesProvider implements DataSourcesProvider {
    private static final int DEFAULT_ZK_PORT = 2181;
    private static final String CONFIG_KEY_PRODUCER = "producer";
    private static final String CONFIG_KEY_BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String URI_PARAMS_TOPIC_KEY = "topic";

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
        private final SpoutConfig conf;
        private final String topic;
        private final int primaryKeyIndex;
        private final Properties props;
        private final IOutputSerializer serializer;

        private KafkaStreamsDataSource(SpoutConfig conf, String topic, int primaryKeyIndex,
                                       Properties props, IOutputSerializer serializer) {
            this.conf = conf;
            this.topic = topic;
            this.primaryKeyIndex = primaryKeyIndex;
            this.props = props;
            this.serializer = serializer;
        }

        @Override
        public IRichSpout getProducer() {
            return new KafkaSpout(conf);
        }

        @Override
        public IRichBolt getConsumer() {
            Preconditions.checkArgument(!props.isEmpty(),
                    "Writable Kafka Table " + topic + " must contain producer config");
            HashMap<String, Object> producerConfig = (HashMap<String, Object>) props.get(CONFIG_KEY_PRODUCER);
            props.putAll(producerConfig);
            Preconditions.checkState(props.containsKey(CONFIG_KEY_BOOTSTRAP_SERVERS),
                    "Writable Kafka Table " + topic + " must contain \"bootstrap.servers\" config");

            TupleToKafkaMapper<Object, ByteBuffer> mapper = new SqlKafkaMapper(serializer);

            return new KafkaBolt()
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
        int port = uri.getPort() != -1 ? uri.getPort() : DEFAULT_ZK_PORT;
        ZkHosts zk = new ZkHosts(uri.getHost() + ":" + port, uri.getPath());
        Map<String, String> values = parseUriParams(uri.getQuery());
        String topic = values.get(URI_PARAMS_TOPIC_KEY);
        Preconditions.checkNotNull(topic, "No topic of the spout is specified");
        SpoutConfig conf = new SpoutConfig(zk, topic, uri.getPath(), UUID.randomUUID().toString());
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
        conf.scheme = new SchemeAsMultiScheme(scheme);
        IOutputSerializer serializer = SerdeUtils.getSerializer(outputFormatClass, properties, fieldNames);

        return new KafkaStreamsDataSource(conf, topic, primaryIndex, properties, serializer);
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
