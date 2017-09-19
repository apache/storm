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
package org.apache.storm.sql.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.storm.mongodb.common.MongoDbClient;
import org.apache.storm.mongodb.trident.state.MongoState;
import org.apache.storm.mongodb.trident.state.MongoStateFactory;
import org.apache.storm.mongodb.trident.state.MongoStateUpdater;
import org.apache.storm.sql.runtime.DataSourcesRegistry;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.serde.json.JsonSerializer;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.internal.util.reflection.Whitebox;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestMongoDataSourcesProvider {
  private static final List<FieldInfo> FIELDS = ImmutableList.of(
      new FieldInfo("ID", int.class, true),
      new FieldInfo("val", String.class, false));
  private static final List<String> FIELD_NAMES = ImmutableList.of("ID", "val");
  private static final JsonSerializer SERIALIZER = new JsonSerializer(FIELD_NAMES);
  private static final Properties TBL_PROPERTIES = new Properties();

  static {
    TBL_PROPERTIES.put("collection.name", "collection1");
    TBL_PROPERTIES.put("trident.ser.field", "tridentSerField");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMongoSink() {
    ISqlTridentDataSource ds = DataSourcesRegistry.constructTridentDataSource(
            URI.create("mongodb://127.0.0.1:27017/test"), null, null, TBL_PROPERTIES, FIELDS);
    Assert.assertNotNull(ds);

    ISqlTridentDataSource.SqlTridentConsumer consumer = ds.getConsumer();

    Assert.assertEquals(MongoStateFactory.class, consumer.getStateFactory().getClass());
    Assert.assertEquals(MongoStateUpdater.class, consumer.getStateUpdater().getClass());

    MongoState state = (MongoState) consumer.getStateFactory().makeState(Collections.emptyMap(), null, 0, 1);
    StateUpdater stateUpdater = consumer.getStateUpdater();

    MongoDbClient mongoClient = mock(MongoDbClient.class);
    Whitebox.setInternalState(state, "mongoClient", mongoClient);

    List<TridentTuple> tupleList = mockTupleList();

    for (TridentTuple t : tupleList) {
      stateUpdater.updateState(state, Collections.singletonList(t), null);
      verify(mongoClient).insert(argThat(new MongoArgMatcher(t)) , eq(true));
    }

    verifyNoMoreInteractions(mongoClient);
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

  private static class MongoArgMatcher extends ArgumentMatcher<List<Document>> {
    private final TridentTuple tuple;

    private MongoArgMatcher(TridentTuple tuple) {
      this.tuple = tuple;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean matches(Object o) {
      Document doc = ((List<Document>)o).get(0);
      ByteBuffer buf = ByteBuffer.wrap((byte[])doc.get(TBL_PROPERTIES.getProperty("trident.ser.field")));
      ByteBuffer b = SERIALIZER.write(tuple.getValues(), null);
      return b.equals(buf);
    }
  }
}
