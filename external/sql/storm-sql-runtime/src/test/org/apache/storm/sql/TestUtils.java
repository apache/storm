/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  * <p>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.storm.sql;

import org.apache.storm.sql.runtime.ChannelContext;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.SimpleSqlTridentConsumer;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class TestUtils {
  public static class MyPlus {
    public static Integer evaluate(Integer x, Integer y) {
      return x + y;
    }
  }

  public static class MyConcat {
    public static String init() {
      return "";
    }
    public static String add(String accumulator, String val) {
      return accumulator + val;
    }
    public static String result(String accumulator) {
      return accumulator;
    }
  }


  public static class TopN {
    public static PriorityQueue<Integer> init() {
      return new PriorityQueue<>();
    }
    public static PriorityQueue<Integer> add(PriorityQueue<Integer> accumulator, Integer n, Integer val) {
      if (n <= 0) {
        return accumulator;
      }
      if (accumulator.size() >= n) {
        if (val > accumulator.peek()) {
          accumulator.remove();
          accumulator.add(val);
        }
      } else {
        accumulator.add(val);
      }
      return accumulator;
    }
    public static List<Integer> result(PriorityQueue<Integer> accumulator) {
      List<Integer> res = new ArrayList<>(accumulator);
      Collections.reverse(res);
      return res;
    }
  }


  public static class MockDataSource implements DataSource {
    private final ArrayList<Values> RECORDS = new ArrayList<>();

    public MockDataSource() {
      for (int i = 0; i < 5; ++i) {
        RECORDS.add(new Values(i, "x", null));
      }
    }

    @Override
    public void open(ChannelContext ctx) {
      for (Values v : RECORDS) {
        ctx.emit(v);
      }
      ctx.fireChannelInactive();
    }
  }

  public static class MockGroupDataSource implements DataSource {
    private final ArrayList<Values> RECORDS = new ArrayList<>();

    public MockGroupDataSource() {
      for (int i = 0; i < 10; ++i) {
        RECORDS.add(new Values(i/3, i, (i+1)* 0.5, "x", i/2));
      }
    }

    @Override
    public void open(ChannelContext ctx) {
      for (Values v : RECORDS) {
        ctx.emit(v);
      }
      // force evaluation of the aggregate function on the last group
      ctx.flush();
      ctx.fireChannelInactive();
    }
  }

  public static class MockEmpDataSource implements DataSource {
    private final ArrayList<Values> RECORDS = new ArrayList<>();

    public MockEmpDataSource() {
      RECORDS.add(new Values(1, "emp1", 1));
      RECORDS.add(new Values(2, "emp2", 1));
      RECORDS.add(new Values(3, "emp3", 2));
    }

    @Override
    public void open(ChannelContext ctx) {
      for (Values v : RECORDS) {
        ctx.emit(v);
      }
      ctx.flush();
      ctx.fireChannelInactive();
    }
  }

  public static class MockDeptDataSource implements DataSource {
    private final ArrayList<Values> RECORDS = new ArrayList<>();

    public MockDeptDataSource() {
      RECORDS.add(new Values(1, "dept1"));
      RECORDS.add(new Values(2, "dept2"));
      RECORDS.add(new Values(3, "dept3"));
    }

    @Override
    public void open(ChannelContext ctx) {
      for (Values v : RECORDS) {
        ctx.emit(v);
      }
      ctx.flush();
      ctx.fireChannelInactive();
    }
  }

  public static class MockNestedDataSource implements DataSource {
    private final ArrayList<Values> RECORDS = new ArrayList<>();

    public MockNestedDataSource() {
      List<Integer> ints = Arrays.asList(100, 200, 300);
      for (int i = 0; i < 5; ++i) {
        Map<String, Integer> map = new HashMap<>();
        map.put("b", i);
        map.put("c", i*i);
        Map<String, Map<String, Integer>> mm = new HashMap<>();
        mm.put("a", map);
        RECORDS.add(new Values(i, map, mm, ints));
      }
    }

    @Override
    public void open(ChannelContext ctx) {
      for (Values v : RECORDS) {
        ctx.emit(v);
      }
      ctx.fireChannelInactive();
    }
  }

  public static class MockState implements State {
    /**
     * Collect all values in a static variable as the instance will go through serialization and deserialization.
     * NOTE: This should be cleared before or after running each test.
     */
    private transient static final List<List<Object> > VALUES = new ArrayList<>();

    public static List<List<Object>> getCollectedValues() {
      return VALUES;
    }

    @Override
    public void beginCommit(Long txid) {
      // NOOP
    }

    @Override
    public void commit(Long txid) {
      // NOOP
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
      for (TridentTuple tuple : tuples) {
        VALUES.add(tuple.getValues());
      }
    }
  }

  public static class MockStateFactory implements StateFactory {

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
      return new MockState();
    }
  }

  public static class MockStateUpdater implements StateUpdater<MockState> {

    @Override
    public void updateState(MockState state, List<TridentTuple> tuples, TridentCollector collector) {
      state.updateState(tuples, collector);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
      // NOOP
    }

    @Override
    public void cleanup() {
      // NOOP
    }
  }

  public static class MockSqlTridentDataSource implements ISqlTridentDataSource {
    @Override
    public IBatchSpout getProducer() {
      return new MockSpout();
    }

    @Override
    public SqlTridentConsumer getConsumer() {
      return new SimpleSqlTridentConsumer(new MockStateFactory(), new MockStateUpdater());
    }

    private static class MockSpout implements IBatchSpout {
      private final ArrayList<Values> RECORDS = new ArrayList<>();
      private final Fields OUTPUT_FIELDS = new Fields("ID", "NAME", "ADDR");

      public MockSpout() {
        RECORDS.add(new Values(0, "a", "y"));
        RECORDS.add(new Values(1, "ab", "y"));
        RECORDS.add(new Values(2, "abc", "y"));
        RECORDS.add(new Values(3, "abcd", "y"));
        RECORDS.add(new Values(4, "abcde", "y"));
      }

      private boolean emitted = false;

      @Override
      public void open(Map conf, TopologyContext context) {
      }

      @Override
      public void emitBatch(long batchId, TridentCollector collector) {
        if (emitted) {
          return;
        }

        for (Values r : RECORDS) {
          collector.emit(r);
        }
        emitted = true;
      }

      @Override
      public void ack(long batchId) {
      }

      @Override
      public void close() {
      }

      @Override
      public Map<String, Object> getComponentConfiguration() {
        return null;
      }

      @Override
      public Fields getOutputFields() {
        return OUTPUT_FIELDS;
      }
    }
  }

  public static class MockSqlTridentGroupedDataSource implements ISqlTridentDataSource {
    @Override
    public IBatchSpout getProducer() {
      return new MockGroupedSpout();
    }

    @Override
    public SqlTridentConsumer getConsumer() {
      return new SimpleSqlTridentConsumer(new MockStateFactory(), new MockStateUpdater());
    }

    private static class MockGroupedSpout implements IBatchSpout {
      private final ArrayList<Values> RECORDS = new ArrayList<>();
      private final Fields OUTPUT_FIELDS = new Fields("ID", "GRPID", "NAME", "ADDR", "AGE", "SCORE");

      public MockGroupedSpout() {
        for (int i = 0; i < 5; ++i) {
          RECORDS.add(new Values(i, 0, "x", "y", 5 - i, i * 10));
        }
      }

      private boolean emitted = false;

      @Override
      public void open(Map conf, TopologyContext context) {
      }

      @Override
      public void emitBatch(long batchId, TridentCollector collector) {
        if (emitted) {
          return;
        }

        for (Values r : RECORDS) {
          collector.emit(r);
        }
        emitted = true;
      }

      @Override
      public void ack(long batchId) {
      }

      @Override
      public void close() {
      }

      @Override
      public Map<String, Object> getComponentConfiguration() {
        return null;
      }

      @Override
      public Fields getOutputFields() {
        return OUTPUT_FIELDS;
      }
    }
  }

  public static class MockSqlTridentJoinDataSourceEmp implements ISqlTridentDataSource {
    @Override
    public IBatchSpout getProducer() {
      return new MockSpout();
    }

    @Override
    public SqlTridentConsumer getConsumer() {
      return new SimpleSqlTridentConsumer(new MockStateFactory(), new MockStateUpdater());
    }

    private static class MockSpout implements IBatchSpout {
      private final ArrayList<Values> RECORDS = new ArrayList<>();
      private final Fields OUTPUT_FIELDS = new Fields("EMPID", "EMPNAME", "DEPTID");

      public MockSpout() {
        for (int i = 0; i < 5; ++i) {
          RECORDS.add(new Values(i, "emp-" + i, i % 2));
        }
        for (int i = 10; i < 15; ++i) {
          RECORDS.add(new Values(i, "emp-" + i, i));
        }
      }

      private boolean emitted = false;

      @Override
      public void open(Map conf, TopologyContext context) {
      }

      @Override
      public void emitBatch(long batchId, TridentCollector collector) {
        if (emitted) {
          return;
        }

        for (Values r : RECORDS) {
          collector.emit(r);
        }
        emitted = true;
      }

      @Override
      public void ack(long batchId) {
      }

      @Override
      public void close() {
      }

      @Override
      public Map<String, Object> getComponentConfiguration() {
        return null;
      }

      @Override
      public Fields getOutputFields() {
        return OUTPUT_FIELDS;
      }
    }
  }

  public static class MockSqlTridentJoinDataSourceDept implements ISqlTridentDataSource {
    @Override
    public IBatchSpout getProducer() {
      return new MockSpout();
    }

    @Override
    public SqlTridentConsumer getConsumer() {
      return new SimpleSqlTridentConsumer(new MockStateFactory(), new MockStateUpdater());
    }

    private static class MockSpout implements IBatchSpout {
      private final ArrayList<Values> RECORDS = new ArrayList<>();
      private final Fields OUTPUT_FIELDS = new Fields("DEPTID", "DEPTNAME");

      public MockSpout() {
        for (int i = 0; i < 5; ++i) {
          RECORDS.add(new Values(i, "dept-" + i));
        }
      }

      private boolean emitted = false;

      @Override
      public void open(Map conf, TopologyContext context) {
      }

      @Override
      public void emitBatch(long batchId, TridentCollector collector) {
        if (emitted) {
          return;
        }

        for (Values r : RECORDS) {
          collector.emit(r);
        }
        emitted = true;
      }

      @Override
      public void ack(long batchId) {
      }

      @Override
      public void close() {
      }

      @Override
      public Map<String, Object> getComponentConfiguration() {
        return null;
      }

      @Override
      public Fields getOutputFields() {
        return OUTPUT_FIELDS;
      }
    }
  }

  public static class MockSqlTridentNestedDataSource implements ISqlTridentDataSource {
    @Override
    public IBatchSpout getProducer() {
      return new MockSpout();
    }

    @Override
    public SqlTridentConsumer getConsumer() {
      return new SimpleSqlTridentConsumer(new MockStateFactory(), new MockStateUpdater());
    }

    private static class MockSpout implements IBatchSpout {
      private final ArrayList<Values> RECORDS = new ArrayList<>();
      private final Fields OUTPUT_FIELDS = new Fields("ID", "MAPFIELD", "NESTEDMAPFIELD", "ARRAYFIELD");

      public MockSpout() {
        List<Integer> ints = Arrays.asList(100, 200, 300);
        for (int i = 0; i < 5; ++i) {
          Map<String, Integer> map = new HashMap<>();
          map.put("b", i);
          map.put("c", i*i);
          Map<String, Map<String, Integer>> mm = new HashMap<>();
          mm.put("a", map);
          RECORDS.add(new Values(i, map, mm, ints));
        }
      }

      private boolean emitted = false;

      @Override
      public void open(Map conf, TopologyContext context) {
      }

      @Override
      public void emitBatch(long batchId, TridentCollector collector) {
        if (emitted) {
          return;
        }

        for (Values r : RECORDS) {
          collector.emit(r);
        }
        emitted = true;
      }

      @Override
      public void ack(long batchId) {
      }

      @Override
      public void close() {
      }

      @Override
      public Map<String, Object> getComponentConfiguration() {
        return null;
      }

      @Override
      public Fields getOutputFields() {
        return OUTPUT_FIELDS;
      }
    }
  }

  public static class CollectDataChannelHandler implements ChannelHandler {
    private final List<Values> values;

    public CollectDataChannelHandler(List<Values> values) {
      this.values = values;
    }

    @Override
    public void dataReceived(ChannelContext ctx, Values data) {
      values.add(data);
    }

    @Override
    public void channelInactive(ChannelContext ctx) {}

    @Override
    public void exceptionCaught(Throwable cause) {
      throw new RuntimeException(cause);
    }

    @Override
    public void flush(ChannelContext ctx) {}

    @Override
    public void setSource(ChannelContext ctx, Object source) {}
  }

  public static long monotonicNow() {
    final long NANOSECONDS_PER_MILLISECOND = 1000000;
    return System.nanoTime() / NANOSECONDS_PER_MILLISECOND;
  }
}
