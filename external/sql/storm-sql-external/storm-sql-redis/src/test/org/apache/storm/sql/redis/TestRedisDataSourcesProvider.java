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
package org.apache.storm.sql.redis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.storm.redis.trident.state.RedisClusterState;
import org.apache.storm.redis.trident.state.RedisClusterStateUpdater;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.redis.trident.state.RedisStateUpdater;
import org.apache.storm.sql.runtime.DataSourcesRegistry;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.serde.json.JsonSerializer;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRedisDataSourcesProvider {
  private static final List<FieldInfo> FIELDS = ImmutableList.of(
      new FieldInfo("ID", int.class, true),
      new FieldInfo("val", String.class, false));
  private static final List<String> FIELD_NAMES = ImmutableList.of("ID", "val");
  private static final String ADDITIONAL_KEY = "hello";
  private static final JsonSerializer SERIALIZER = new JsonSerializer(FIELD_NAMES);
  private static final Properties TBL_PROPERTIES = new Properties();
  private static final Properties CLUSTER_TBL_PROPERTIES = new Properties();

  static {
    TBL_PROPERTIES.put("data.type", "HASH");
    TBL_PROPERTIES.put("data.additional.key", ADDITIONAL_KEY);
    CLUSTER_TBL_PROPERTIES.put("data.type", "HASH");
    CLUSTER_TBL_PROPERTIES.put("data.additional.key", ADDITIONAL_KEY);
    CLUSTER_TBL_PROPERTIES.put("use.redis.cluster", "true");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRedisSink() {
    ISqlTridentDataSource ds = DataSourcesRegistry.constructTridentDataSource(
        URI.create("redis://:foobared@localhost:6380/2"), null, null, TBL_PROPERTIES, FIELDS);
    Assert.assertNotNull(ds);

    ISqlTridentDataSource.SqlTridentConsumer consumer = ds.getConsumer();

    Assert.assertEquals(RedisState.Factory.class, consumer.getStateFactory().getClass());
    Assert.assertEquals(RedisStateUpdater.class, consumer.getStateUpdater().getClass());

    RedisState state = (RedisState) consumer.getStateFactory().makeState(Collections.emptyMap(), null, 0, 1);
    StateUpdater stateUpdater = consumer.getStateUpdater();

    JedisPool mockJedisPool = mock(JedisPool.class);
    Jedis mockJedis = mock(Jedis.class);
    Pipeline mockPipeline = mock(Pipeline.class);

    Whitebox.setInternalState(state, "jedisPool", mockJedisPool);
    when(mockJedisPool.getResource()).thenReturn(mockJedis);
    when(mockJedis.pipelined()).thenReturn(mockPipeline);

    List<TridentTuple> tupleList = mockTupleList();

    stateUpdater.updateState(state, tupleList, null);
    for (TridentTuple t : tupleList) {
      // PK goes to the key
      String id = String.valueOf(t.getValueByField("ID"));
      String serializedValue = new String(SERIALIZER.write(t.getValues(), null).array());
      verify(mockPipeline).hset(eq(ADDITIONAL_KEY), eq(id), eq(serializedValue));
    }

    verify(mockPipeline).sync();
    verify(mockJedis).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRedisClusterSink() throws IOException {
    ISqlTridentDataSource ds = DataSourcesRegistry.constructTridentDataSource(
        URI.create("redis://localhost:6380"), null, null, CLUSTER_TBL_PROPERTIES, FIELDS);
    Assert.assertNotNull(ds);

    ISqlTridentDataSource.SqlTridentConsumer consumer = ds.getConsumer();

    Assert.assertEquals(RedisClusterState.Factory.class, consumer.getStateFactory().getClass());
    Assert.assertEquals(RedisClusterStateUpdater.class, consumer.getStateUpdater().getClass());

    RedisClusterState state = (RedisClusterState) consumer.getStateFactory().makeState(Collections.emptyMap(), null, 0, 1);
    StateUpdater stateUpdater = consumer.getStateUpdater();

    JedisCluster mockJedisCluster = mock(JedisCluster.class);

    Whitebox.setInternalState(state, "jedisCluster", mockJedisCluster);

    List<TridentTuple> tupleList = mockTupleList();

    stateUpdater.updateState(state, tupleList, null);
    for (TridentTuple t : tupleList) {
      // PK goes to the key
      String id = String.valueOf(t.getValueByField("ID"));
      String serializedValue = new String(SERIALIZER.write(t.getValues(), null).array());
      verify(mockJedisCluster).hset(eq(ADDITIONAL_KEY), eq(id), eq(serializedValue));
    }

    verify(mockJedisCluster, never()).close();
  }

  private static List<TridentTuple> mockTupleList() {
    List<TridentTuple> tupleList = new ArrayList<>();
    TridentTuple t0 = mock(TridentTuple.class);
    TridentTuple t1 = mock(TridentTuple.class);
    when(t0.getValueByField("ID")).thenReturn(1);
    when(t0.getValueByField("val")).thenReturn("2");
    doReturn(Lists.<Object>newArrayList(1, "2")).when(t0).getValues();

    when(t1.getValueByField("ID")).thenReturn(2);
    when(t1.getValueByField("val")).thenReturn("3");
    doReturn(Lists.<Object>newArrayList(2, "3")).when(t1).getValues();

    tupleList.add(t0);
    tupleList.add(t1);
    return tupleList;
  }

}
