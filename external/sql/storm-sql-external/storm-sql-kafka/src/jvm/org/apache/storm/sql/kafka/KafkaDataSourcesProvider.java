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
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaUpdater;
import org.apache.storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.IOutputSerializer;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.SimpleSqlTridentConsumer;
import org.apache.storm.sql.runtime.utils.SerdeUtils;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.tuple.TridentTuple;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Create a Kafka spout/sink based on the URI and properties. The URI has the format of
 * kafka://zkhost:port/broker_path?topic=topic. The properties are in JSON format which specifies the producer config
 * of the Kafka broker.
 */
public class KafkaDataSourcesProvider implements DataSourcesProvider {
  private static final int DEFAULT_ZK_PORT = 2181;

  private static class SqlKafkaMapper implements TridentTupleToKafkaMapper<Object, ByteBuffer> {
    private final int primaryKeyIndex;
    private final IOutputSerializer serializer;

    private SqlKafkaMapper(int primaryKeyIndex, IOutputSerializer serializer) {
      this.primaryKeyIndex = primaryKeyIndex;
      this.serializer = serializer;
    }

    @Override
    public Object getKeyFromTuple(TridentTuple tuple) {
      return tuple.get(primaryKeyIndex);
    }

    @Override
    public ByteBuffer getMessageFromTuple(TridentTuple tuple) {
      return serializer.write(tuple.getValues(), null);
    }
  }

  private static class KafkaTridentDataSource implements ISqlTridentDataSource {
    private final TridentKafkaConfig conf;
    private final String topic;
    private final int primaryKeyIndex;
    private final Properties props;
    private final IOutputSerializer serializer;
    private KafkaTridentDataSource(TridentKafkaConfig conf, String topic, int primaryKeyIndex,
                                   Properties props, IOutputSerializer serializer) {
      this.conf = conf;
      this.topic = topic;
      this.primaryKeyIndex = primaryKeyIndex;
      this.props = props;
      this.serializer = serializer;
    }

    @Override
    public ITridentDataSource getProducer() {
      return new OpaqueTridentKafkaSpout(conf);
    }

    @Override
    public SqlTridentConsumer getConsumer() {
      Preconditions.checkArgument(!props.isEmpty(),
              "Writable Kafka Table " + topic + " must contain producer config");
      HashMap<String, Object> producerConfig = (HashMap<String, Object>) props.get("producer");
      props.putAll(producerConfig);
      Preconditions.checkState(props.containsKey("bootstrap.servers"),
              "Writable Kafka Table " + topic + " must contain \"bootstrap.servers\" config");

      SqlKafkaMapper mapper = new SqlKafkaMapper(primaryKeyIndex, serializer);

      TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
              .withKafkaTopicSelector(new DefaultTopicSelector(topic))
              .withProducerProperties(props)
              .withTridentTupleToKafkaMapper(mapper);

      TridentKafkaUpdater stateUpdater = new TridentKafkaUpdater();

      return new SimpleSqlTridentConsumer(stateFactory, stateUpdater);
    }
  }

  @Override
  public String scheme() {
    return "kafka";
  }

  @Override
  public DataSource construct(URI uri, String inputFormatClass, String outputFormatClass,
                              List<FieldInfo> fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ISqlTridentDataSource constructTrident(URI uri, String inputFormatClass, String outputFormatClass,
                                                Properties properties, List<FieldInfo> fields) {
    int port = uri.getPort() != -1 ? uri.getPort() : DEFAULT_ZK_PORT;
    ZkHosts zk = new ZkHosts(uri.getHost() + ":" + port, uri.getPath());
    Map<String, String> values = parseURIParams(uri.getQuery());
    String topic = values.get("topic");
    Preconditions.checkNotNull(topic, "No topic of the spout is specified");
    TridentKafkaConfig conf = new TridentKafkaConfig(zk, topic);
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

    return new KafkaTridentDataSource(conf, topic, primaryIndex, properties, serializer);
  }

  private static Map<String, String> parseURIParams(String query) {
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
