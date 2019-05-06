/*
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
 *
 */
package org.apache.storm.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.streams.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class TestUtils {
    public static final class MockInsertBoltExtension implements BeforeEachCallback {
        @Override
        public void beforeEach(ExtensionContext ctx) throws Exception {
            MockInsertBolt.getCollectedValues().clear();
        }
    }

    public static final class MockBoltExtension implements BeforeEachCallback {
        @Override
        public void beforeEach(ExtensionContext arg0) throws Exception {
            MockBolt.getCollectedValues().clear();
        }
    }

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

  public static class MockSpout extends BaseRichSpout {

    private final List<Values> records;
    private final Fields outputFields;
    private boolean emitted = false;
    private SpoutOutputCollector collector;

    public MockSpout(List<Values> records, Fields outputFields) {
      this.records = records;
      this.outputFields = outputFields;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
      this.collector = collector;
    }

    @Override
    public void nextTuple() {
      if (emitted) {
        return;
      }

      for (Values r : records) {
        collector.emit(r);
      }

      emitted = true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(outputFields);
    }
  }

  public static class MockBolt extends BaseRichBolt {
    /**
     * Collect all values in a static variable as the instance will go through serialization and deserialization.
     * NOTE: This should be cleared before or after running each test.
     */
    private transient static final List<Values> VALUES = new ArrayList<>();

    public static List<Values> getCollectedValues() {
      return VALUES;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
      VALUES.add((Values) input.getValue(0));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
  }

  public static class MockInsertBolt extends BaseRichBolt {
    /**
     * Collect all values in a static variable as the instance will go through serialization and deserialization.
     * NOTE: This should be cleared before or after running each test.
     */
    private transient static final List<Pair<Object, Values>> VALUES = new ArrayList<>();

    public static List<Pair<Object, Values>> getCollectedValues() {
      return VALUES;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
      VALUES.add(Pair.of(input.getValue(0), (Values) input.getValue(1)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
  }

  public static class MockSqlExprDataSource implements ISqlStreamsDataSource {
    @Override
    public IRichSpout getProducer() {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public IRichBolt getConsumer() {
      return new MockBolt();
    }
  }

  public static class MockSqlStreamsOutputDataSource implements ISqlStreamsDataSource {

    @Override
    public IRichSpout getProducer() {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public IRichBolt getConsumer() {
      return new MockInsertBolt();
    }

  }

  public static class MockSqlStreamsDataSource implements ISqlStreamsDataSource {
    @Override
    public IRichSpout getProducer() {
      List<Values> records = new ArrayList<>();
      records.add(new Values(0, "a", "y"));
      records.add(new Values(1, "ab", "y"));
      records.add(new Values(2, "abc", "y"));
      records.add(new Values(3, "abcd", "y"));
      records.add(new Values(4, "abcde", "y"));

      Fields outputFields = new Fields("ID", "NAME", "ADDR");
      return new MockSpout(records, outputFields);
    }

    @Override
    public IRichBolt getConsumer() {
      return new MockBolt();
    }

  }

  public static class MockSqlStreamsInsertDataSource extends MockSqlStreamsNestedDataSource {
    @Override
    public IRichBolt getConsumer() {
      return new MockInsertBolt();
    }
  }

  public static class MockSqlStreamsGroupedDataSource implements ISqlStreamsDataSource {
    @Override
    public IRichSpout getProducer() {
      List<Values> records = new ArrayList<>();
      for (int i = 0; i < 5; ++i) {
        records.add(new Values(i, 0, "x", "y", 5 - i, i * 10));
      }

      Fields outputFields = new Fields("ID", "GRPID", "NAME", "ADDR", "AGE", "SCORE");
      return new MockSpout(records, outputFields);
    }

    @Override
    public IRichBolt getConsumer() {
      return new MockBolt();
    }
  }

  public static class MockSqlStreamsInsertGroupedDataSource extends MockSqlStreamsGroupedDataSource {
    @Override
    public IRichBolt getConsumer() {
      return new MockInsertBolt();
    }
  }

  public static class MockSqlStreamsJoinDataSourceEmp implements ISqlStreamsDataSource {
    @Override
    public IRichSpout getProducer() {
      List<Values> records = new ArrayList<>();
      Fields outputFields = new Fields("EMPID", "EMPNAME", "DEPTID");

      for (int i = 0; i < 5; ++i) {
        records.add(new Values(i, "emp-" + i, i % 2));
      }
      for (int i = 10; i < 15; ++i) {
        records.add(new Values(i, "emp-" + i, i));
      }

      return new MockSpout(records, outputFields);
    }

    @Override
    public IRichBolt getConsumer() {
      return new MockBolt();
    }
  }

  public static class MockSqlStreamsInsertJoinDataSourceEmp extends MockSqlStreamsJoinDataSourceEmp {
    @Override
    public IRichBolt getConsumer() {
      return new MockInsertBolt();
    }
  }

  public static class MockSqlStreamsJoinDataSourceDept implements ISqlStreamsDataSource {
    @Override
    public IRichSpout getProducer() {
      List<Values> records = new ArrayList<>();
      Fields outputFields = new Fields("DEPTID", "DEPTNAME");

      for (int i = 0; i < 5; ++i) {
        records.add(new Values(i, "dept-" + i));
      }

      return new MockSpout(records, outputFields);
    }

    @Override
    public IRichBolt getConsumer() {
      return new MockBolt();
    }
  }

  public static class MockSqlStreamsInsertJoinDataSourceDept extends MockSqlStreamsJoinDataSourceDept {
    @Override
    public IRichBolt getConsumer() {
      return new MockInsertBolt();
    }
  }

  public static class MockSqlStreamsNestedDataSource implements ISqlStreamsDataSource {
    @Override
    public IRichSpout getProducer() {
      List<Values> records = new ArrayList<>();
      Fields outputFields = new Fields("ID", "MAPFIELD", "NESTEDMAPFIELD", "ARRAYFIELD");

      List<Integer> ints = Arrays.asList(100, 200, 300);
      for (int i = 0; i < 5; ++i) {
        Map<String, Integer> map = new HashMap<>();
        map.put("b", i);
        map.put("c", i*i);
        Map<String, Map<String, Integer>> mm = new HashMap<>();
        mm.put("a", map);
        records.add(new Values(i, map, mm, ints));
      }

      return new MockSpout(records, outputFields);
    }

    @Override
    public IRichBolt getConsumer() {
      return new MockBolt();
    }
  }

  public static class MockSqlStreamsInsertNestedDataSource extends MockSqlStreamsNestedDataSource {
    @Override
    public IRichBolt getConsumer() {
      return new MockInsertBolt();
    }
  }

  public static long monotonicNow() {
    final long NANOSECONDS_PER_MILLISECOND = 1000000;
    return System.nanoTime() / NANOSECONDS_PER_MILLISECOND;
  }
}