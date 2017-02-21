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
package org.apache.storm.sql.compiler.backends.trident;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalCluster.LocalTopology;
import org.apache.storm.sql.TestUtils;
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.sql.planner.trident.QueryPlanner;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.AbstractTridentProcessor;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.storm.sql.TestUtils.MockState.getCollectedValues;

public class TestPlanCompiler {
  private static LocalCluster cluster;

  @BeforeClass
  public static void staticSetup() throws Exception {
    cluster = new LocalCluster();
  }

  @AfterClass
  public static void staticCleanup() {
    if (cluster!= null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Before
  public void setUp() {
    getCollectedValues().clear();
  }

  @Test
  public void testCompile() throws Exception {
    final int EXPECTED_VALUE_SIZE = 2;
    String sql = "SELECT ID FROM FOO WHERE ID > 2";
    TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
    final Map<String, ISqlTridentDataSource> data = new HashMap<>();
    data.put("FOO", new TestUtils.MockSqlTridentDataSource());
    QueryPlanner planner = new QueryPlanner(state.schema());
    AbstractTridentProcessor proc = planner.compile(data, sql);
    final TridentTopology topo = proc.build();
    Fields f = proc.outputStream().getOutputFields();
    proc.outputStream().partitionPersist(new TestUtils.MockStateFactory(),
            f, new TestUtils.MockStateUpdater(), new Fields());
    runTridentTopology(EXPECTED_VALUE_SIZE, proc, topo);
    Assert.assertArrayEquals(new Values[] { new Values(3), new Values(4)}, getCollectedValues().toArray());
  }

  @Test
  public void testInsert() throws Exception {
    final int EXPECTED_VALUE_SIZE = 1;
    String sql = "INSERT INTO BAR SELECT ID, NAME, ADDR FROM FOO WHERE ID > 3";
    TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
    final Map<String, ISqlTridentDataSource> data = new HashMap<>();
    data.put("FOO", new TestUtils.MockSqlTridentDataSource());
    data.put("BAR", new TestUtils.MockSqlTridentDataSource());

    QueryPlanner planner = new QueryPlanner(state.schema());
    AbstractTridentProcessor proc = planner.compile(data, sql);
    final TridentTopology topo = proc.build();
    runTridentTopology(EXPECTED_VALUE_SIZE, proc, topo);
    Assert.assertArrayEquals(new Values[] { new Values(4, "abcde", "y")}, getCollectedValues().toArray());
  }

  @Test
  public void testUdf() throws Exception {
    int EXPECTED_VALUE_SIZE = 1;
    String sql = "SELECT MYPLUS(ID, 3)" +
            "FROM FOO " +
            "WHERE ID = 2";
    TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
    Map<String, ISqlTridentDataSource> data = new HashMap<>();
    data.put("FOO", new TestUtils.MockSqlTridentDataSource());

    QueryPlanner planner = new QueryPlanner(state.schema());
    AbstractTridentProcessor proc = planner.compile(data, sql);
    final TridentTopology topo = proc.build();
    Fields f = proc.outputStream().getOutputFields();
    proc.outputStream().partitionPersist(new TestUtils.MockStateFactory(),
            f, new TestUtils.MockStateUpdater(), new Fields());
    runTridentTopology(EXPECTED_VALUE_SIZE, proc, topo);
    Assert.assertArrayEquals(new Values[] { new Values(5) }, getCollectedValues().toArray());
  }

  @Test
  public void testCaseStatement() throws Exception {
    int EXPECTED_VALUE_SIZE = 5;
    String sql = "SELECT CASE WHEN NAME IN ('a', 'abc', 'abcde') THEN UPPER('a') " +
            "WHEN UPPER(NAME) = 'AB' THEN 'b' ELSE {fn CONCAT(NAME, '#')} END FROM FOO";
    TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);

    final Map<String, ISqlTridentDataSource> data = new HashMap<>();
    data.put("FOO", new TestUtils.MockSqlTridentDataSource());

    QueryPlanner planner = new QueryPlanner(state.schema());
    AbstractTridentProcessor proc = planner.compile(data, sql);
    final TridentTopology topo = proc.build();
    Fields f = proc.outputStream().getOutputFields();
    proc.outputStream().partitionPersist(new TestUtils.MockStateFactory(), f, new TestUtils.MockStateUpdater(), new Fields());
    runTridentTopology(EXPECTED_VALUE_SIZE, proc, topo);

    Assert.assertArrayEquals(new Values[]{new Values("A"), new Values("b"), new Values("A"), new Values("abcd#"), new Values("A")}, getCollectedValues().toArray());
  }

  @Test
  public void testNested() throws Exception {
    int EXPECTED_VALUE_SIZE = 1;
    String sql = "SELECT ID, MAPFIELD['c'], NESTEDMAPFIELD, ARRAYFIELD " +
            "FROM FOO " +
            "WHERE NESTEDMAPFIELD['a']['b'] = 2 AND ARRAYFIELD[2] = 200";
    TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverNestedTable(sql);

    final Map<String, ISqlTridentDataSource> data = new HashMap<>();
    data.put("FOO", new TestUtils.MockSqlTridentNestedDataSource());

    QueryPlanner planner = new QueryPlanner(state.schema());
    AbstractTridentProcessor proc = planner.compile(data, sql);
    final TridentTopology topo = proc.build();
    Fields f = proc.outputStream().getOutputFields();
    proc.outputStream().partitionPersist(new TestUtils.MockStateFactory(), f, new TestUtils.MockStateUpdater(), new Fields());
    runTridentTopology(EXPECTED_VALUE_SIZE, proc, topo);

    Map<String, Integer> map = ImmutableMap.of("b", 2, "c", 4);
    Map<String, Map<String, Integer>> nestedMap = ImmutableMap.of("a", map);
    Assert.assertArrayEquals(new Values[]{new Values(2, 4, nestedMap, Arrays.asList(100, 200, 300))}, getCollectedValues().toArray());
  }

  @Test
  public void testDateKeywords() throws Exception {
    int EXPECTED_VALUE_SIZE = 1;
    String sql = "SELECT " +
            "LOCALTIME, CURRENT_TIME, LOCALTIMESTAMP, CURRENT_TIMESTAMP, CURRENT_DATE " +
            "FROM FOO " +
            "WHERE ID > 0 AND ID < 2";
    TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);

    final Map<String, ISqlTridentDataSource> data = new HashMap<>();
    data.put("FOO", new TestUtils.MockSqlTridentDataSource());
    QueryPlanner planner = new QueryPlanner(state.schema());
    AbstractTridentProcessor proc = planner.compile(data, sql);
    final DataContext dataContext = proc.getDataContext();
    final TridentTopology topo = proc.build();
    Fields f = proc.outputStream().getOutputFields();
    proc.outputStream().partitionPersist(new TestUtils.MockStateFactory(), f, new TestUtils.MockStateUpdater(), new Fields());
    runTridentTopology(EXPECTED_VALUE_SIZE, proc, topo);

    long utcTimestamp = (long) dataContext.get(DataContext.Variable.UTC_TIMESTAMP.camelName);
    long currentTimestamp = (long) dataContext.get(DataContext.Variable.CURRENT_TIMESTAMP.camelName);
    long localTimestamp = (long) dataContext.get(DataContext.Variable.LOCAL_TIMESTAMP.camelName);

    System.out.println(getCollectedValues());

    java.sql.Timestamp timestamp = new java.sql.Timestamp(utcTimestamp);
    int dateInt = (int) timestamp.toLocalDateTime().atOffset(ZoneOffset.UTC).toLocalDate().toEpochDay();
    int localTimeInt = (int) (localTimestamp % DateTimeUtils.MILLIS_PER_DAY);
    int currentTimeInt = (int) (currentTimestamp % DateTimeUtils.MILLIS_PER_DAY);

    Assert.assertArrayEquals(new Values[]{new Values(localTimeInt, currentTimeInt, localTimestamp, currentTimestamp, dateInt)}, getCollectedValues().toArray());
  }

  private void runTridentTopology(final int expectedValueSize, AbstractTridentProcessor proc,
                                  TridentTopology topo) throws Exception {
    final Config conf = new Config();
    conf.setMaxSpoutPending(20);

    if (proc.getClassLoaders() != null && proc.getClassLoaders().size() > 0) {
      CompilingClassLoader lastClassloader = proc.getClassLoaders().get(proc.getClassLoaders().size() - 1);
      Utils.setClassLoaderForJavaDeSerialize(lastClassloader);
    }

    try (LocalTopology stormTopo = cluster.submitTopology("storm-sql", conf, topo.build())) {
      waitForCompletion(1000 * 1000, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return getCollectedValues().size() < expectedValueSize;
        }
      });
    } finally {
      while(cluster.getClusterInfo().get_topologies_size() > 0) {
        Thread.sleep(10);
      }
      Utils.resetClassLoaderForJavaDeSerialize();
    }
  }

  private void waitForCompletion(long timeout, Callable<Boolean> cond) throws Exception {
    long start = TestUtils.monotonicNow();
    while (TestUtils.monotonicNow() - start < timeout && cond.call()) {
      Thread.sleep(100);
    }
  }
}