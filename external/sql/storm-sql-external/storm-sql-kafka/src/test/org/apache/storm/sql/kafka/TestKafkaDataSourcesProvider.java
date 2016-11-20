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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.kafka.trident.TridentKafkaState;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaUpdater;
import org.apache.storm.sql.runtime.DataSourcesRegistry;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.serde.json.JsonSerializer;
import org.apache.storm.trident.tuple.TridentTuple;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.internal.util.reflection.Whitebox;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestKafkaDataSourcesProvider {
  private static final List<FieldInfo> FIELDS = ImmutableList.of(
          new FieldInfo("ID", int.class, true),
          new FieldInfo("val", String.class, false));
  private static final List<String> FIELD_NAMES = ImmutableList.of("ID", "val");
  private static final JsonSerializer SERIALIZER = new JsonSerializer(FIELD_NAMES);
  private static final Properties TBL_PROPERTIES = new Properties();

  static {
    Map<String,Object> map = new HashMap<>();
    map.put("bootstrap.servers", "localhost:9092");
    map.put("acks", "1");
    map.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    map.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    TBL_PROPERTIES.put("producer", map);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testKafkaSink() {
    ISqlTridentDataSource ds = DataSourcesRegistry.constructTridentDataSource(
            URI.create("kafka://mock?topic=foo"), null, null, TBL_PROPERTIES, FIELDS);
    Assert.assertNotNull(ds);

    ISqlTridentDataSource.SqlTridentConsumer consumer = ds.getConsumer();

    Assert.assertEquals(TridentKafkaStateFactory.class, consumer.getStateFactory().getClass());
    Assert.assertEquals(TridentKafkaUpdater.class, consumer.getStateUpdater().getClass());

    TridentKafkaState state = (TridentKafkaState) consumer.getStateFactory().makeState(Collections.emptyMap(), null, 0, 1);
    KafkaProducer producer = mock(KafkaProducer.class);
    doReturn(mock(Future.class)).when(producer).send(any(ProducerRecord.class));
    Whitebox.setInternalState(state, "producer", producer);

    List<TridentTuple> tupleList = mockTupleList();
    for (TridentTuple t : tupleList) {
      state.updateState(Collections.singletonList(t), null);
      verify(producer).send(argThat(new KafkaMessageMatcher(t)));
    }
    verifyNoMoreInteractions(producer);
  }

  private static List<TridentTuple> mockTupleList() {
    List<TridentTuple> tupleList = new ArrayList<>();
    TridentTuple t0 = mock(TridentTuple.class);
    TridentTuple t1 = mock(TridentTuple.class);
    doReturn(1).when(t0).get(0);
    doReturn(2).when(t1).get(0);
    doReturn(Lists.<Object>newArrayList(1, "2")).when(t0).getValues();
    doReturn(Lists.<Object>newArrayList(2, "3")).when(t1).getValues();
    tupleList.add(t0);
    tupleList.add(t1);
    return tupleList;
  }

  private static class KafkaMessageMatcher extends ArgumentMatcher<ProducerRecord> {
    private static final int PRIMARY_INDEX = 0;
    private final TridentTuple tuple;

    private KafkaMessageMatcher(TridentTuple tuple) {
      this.tuple = tuple;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean matches(Object o) {
      ProducerRecord<Object, ByteBuffer> m = (ProducerRecord<Object,ByteBuffer>)o;
      if (m.key() != tuple.get(PRIMARY_INDEX)) {
        return false;
      }
      ByteBuffer buf = m.value();
      ByteBuffer b = SERIALIZER.write(tuple.getValues(), null);
      return b.equals(buf);
    }
  }

}
