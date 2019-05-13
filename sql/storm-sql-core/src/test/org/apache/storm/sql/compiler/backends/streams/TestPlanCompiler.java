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
package org.apache.storm.sql.compiler.backends.streams;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.storm.LocalCluster;
import org.apache.storm.sql.SqlTestUtil;
import org.apache.storm.sql.TestUtils;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.sql.AbstractStreamsProcessor;
import org.apache.storm.sql.planner.streams.QueryPlanner;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.streams.Pair;
import org.apache.storm.tuple.Values;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.MockBoltExtension.class)
@ExtendWith(TestUtils.MockInsertBoltExtension.class)
public class TestPlanCompiler {

    private static LocalCluster cluster;

    @BeforeAll
    public static void staticSetup() throws Exception {
        cluster = new LocalCluster();
    }

    @AfterAll
    public static void staticCleanup() {
        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
        }
    }

    @Test
    public void testCompile() throws Exception {
        final int EXPECTED_VALUE_SIZE = 2;
        String sql = "SELECT ID FROM FOO WHERE ID > 2";
        TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
        final Map<String, ISqlStreamsDataSource> data = new HashMap<>();
        data.put("FOO", new TestUtils.MockSqlStreamsDataSource());

        QueryPlanner planner = new QueryPlanner(state.schema());
        AbstractStreamsProcessor proc = planner.compile(data, sql);
        // inject output bolt
        proc.outputStream().to(new TestUtils.MockBolt());
        final StormTopology topo = proc.build();

        SqlTestUtil.runStormTopology(cluster, TestUtils.MockBolt.getCollectedValues(), EXPECTED_VALUE_SIZE, proc, topo);
        Assert.assertArrayEquals(new Values[]{new Values(3), new Values(4)}, TestUtils.MockBolt.getCollectedValues().toArray());
    }

    @Test
    public void testInsert() throws Exception {
        final int EXPECTED_VALUE_SIZE = 1;
        String sql = "INSERT INTO BAR SELECT ID, NAME, ADDR FROM FOO WHERE ID > 3";
        TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
        final Map<String, ISqlStreamsDataSource> data = new HashMap<>();
        data.put("FOO", new TestUtils.MockSqlStreamsDataSource());
        data.put("BAR", new TestUtils.MockSqlStreamsOutputDataSource());

        QueryPlanner planner = new QueryPlanner(state.schema());
        AbstractStreamsProcessor proc = planner.compile(data, sql);
        final StormTopology topo = proc.build();

        SqlTestUtil.runStormTopology(cluster, TestUtils.MockInsertBolt.getCollectedValues(), EXPECTED_VALUE_SIZE, proc, topo);
        Assert.assertArrayEquals(new Pair[]{Pair.of(4, new Values(4, "abcde", "y"))}, TestUtils.MockInsertBolt.getCollectedValues().toArray());
    }

    @Test
    public void testUdf() throws Exception {
        int EXPECTED_VALUE_SIZE = 1;
        String sql = "SELECT MYPLUS(ID, 3)"
            + "FROM FOO "
            + "WHERE ID = 2";
        TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
        Map<String, ISqlStreamsDataSource> data = new HashMap<>();
        data.put("FOO", new TestUtils.MockSqlStreamsDataSource());

        QueryPlanner planner = new QueryPlanner(state.schema());
        AbstractStreamsProcessor proc = planner.compile(data, sql);
        // inject output bolt
        proc.outputStream().to(new TestUtils.MockBolt());
        final StormTopology topo = proc.build();

        SqlTestUtil.runStormTopology(cluster, TestUtils.MockBolt.getCollectedValues(), EXPECTED_VALUE_SIZE, proc, topo);
        Assert.assertArrayEquals(new Values[]{new Values(5)}, TestUtils.MockBolt.getCollectedValues().toArray());
    }

    @Test
    public void testNested() throws Exception {
        int EXPECTED_VALUE_SIZE = 1;
        String sql = "SELECT ID, MAPFIELD['c'], NESTEDMAPFIELD, ARRAYFIELD "
            + "FROM FOO "
            + "WHERE NESTEDMAPFIELD['a']['b'] = 2 AND ARRAYFIELD[2] = 200";
        TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverNestedTable(sql);

        final Map<String, ISqlStreamsDataSource> data = new HashMap<>();
        data.put("FOO", new TestUtils.MockSqlStreamsNestedDataSource());

        QueryPlanner planner = new QueryPlanner(state.schema());
        AbstractStreamsProcessor proc = planner.compile(data, sql);
        // inject output bolt
        proc.outputStream().to(new TestUtils.MockBolt());
        final StormTopology topo = proc.build();

        SqlTestUtil.runStormTopology(cluster, TestUtils.MockBolt.getCollectedValues(), EXPECTED_VALUE_SIZE, proc, topo);

        Map<String, Integer> map = ImmutableMap.of("b", 2, "c", 4);
        Map<String, Map<String, Integer>> nestedMap = ImmutableMap.of("a", map);
        Assert.assertArrayEquals(new Values[]{new Values(2, 4, nestedMap, Arrays.asList(100, 200, 300))},
            TestUtils.MockBolt.getCollectedValues().toArray());
    }

    /**
     * All the binary literal tests are done here, because Avatica converts the result to byte[] whereas Stream provides the result to
     * ByteString which makes different semantic from Stream implementation.
     */
    @Test
    public void testBinaryStringFunctions() throws Exception {
        int EXPECTED_VALUE_SIZE = 1;
        String sql = "SELECT x'45F0AB' || x'45F0AB', "
            + "POSITION(x'F0' IN x'453423F0ABBC'), "
            + "OVERLAY(x'453423F0ABBC45' PLACING x'4534' FROM 3), "
            + "SUBSTRING(x'453423F0ABBC' FROM 3), "
            + "SUBSTRING(x'453423F0ABBC453423F0ABBC' FROM 3 FOR 4) "
            + "FROM FOO "
            + "WHERE ID > 0 AND ID < 2";

        TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
        final Map<String, ISqlStreamsDataSource> data = new HashMap<>();
        data.put("FOO", new TestUtils.MockSqlStreamsDataSource());

        QueryPlanner planner = new QueryPlanner(state.schema());
        AbstractStreamsProcessor proc = planner.compile(data, sql);
        // inject output bolt
        proc.outputStream().to(new TestUtils.MockBolt());
        final StormTopology topo = proc.build();

        SqlTestUtil.runStormTopology(cluster, TestUtils.MockBolt.getCollectedValues(), EXPECTED_VALUE_SIZE, proc, topo);

        Values v = TestUtils.MockBolt.getCollectedValues().get(0);

        assertEquals("45f0ab45f0ab", v.get(0).toString());
        assertEquals(4, v.get(1));
        assertEquals("45344534abbc45", v.get(2).toString());
        assertEquals("23f0abbc", v.get(3).toString());
        assertEquals("23f0abbc", v.get(4).toString());
    }

    /**
     * All the date/time/timestamp related tests are done here, because Avatica converts the result of date functions to java.sql classes
     * whereas Stream provides long type which makes different semantic from Stream implementation.
     */
    @Test
    public void testDateKeywordsAndFunctions() throws Exception {
        int EXPECTED_VALUE_SIZE = 1;
        String sql = "SELECT "
            + "LOCALTIME, CURRENT_TIME, LOCALTIMESTAMP, CURRENT_TIMESTAMP, CURRENT_DATE, "
            + "DATE '1970-05-15' AS datefield, TIME '00:00:00' AS timefield, TIMESTAMP '2016-01-01 00:00:00' as timestampfield, "
            + "EXTRACT(MONTH FROM TIMESTAMP '2010-01-23 12:34:56'),"
            + "FLOOR(DATE '2016-01-23' TO MONTH),"
            + "CEIL(TIME '12:34:56' TO MINUTE),"
            + "{fn CURDATE()} = CURRENT_DATE, {fn CURTIME()} = LOCALTIME, {fn NOW()} = LOCALTIMESTAMP,"
            + "{fn QUARTER(DATE '2016-10-07')}, {fn TIMESTAMPADD(MINUTE, 15, TIMESTAMP '2016-10-07 00:00:00')},"
            + "{fn TIMESTAMPDIFF(SECOND, TIMESTAMP '2016-10-06 00:00:00', TIMESTAMP '2016-10-07 00:00:00')},"
            + "INTERVAL '1-5' YEAR TO MONTH AS intervalfield, "
            + "(DATE '1970-01-01', DATE '1970-01-15') AS anchoredinterval_field "
            + "FROM FOO "
            + "WHERE ID > 0 AND ID < 2";
        TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);

        final Map<String, ISqlStreamsDataSource> data = new HashMap<>();
        data.put("FOO", new TestUtils.MockSqlStreamsDataSource());

        QueryPlanner planner = new QueryPlanner(state.schema());
        AbstractStreamsProcessor proc = planner.compile(data, sql);
        // inject output bolt
        proc.outputStream().to(new TestUtils.MockBolt());
        final DataContext dataContext = proc.getDataContext();
        final StormTopology topo = proc.build();

        SqlTestUtil.runStormTopology(cluster, TestUtils.MockBolt.getCollectedValues(), EXPECTED_VALUE_SIZE, proc, topo);

        long utcTimestamp = (long) dataContext.get(DataContext.Variable.UTC_TIMESTAMP.camelName);
        long currentTimestamp = (long) dataContext.get(DataContext.Variable.CURRENT_TIMESTAMP.camelName);
        long localTimestamp = (long) dataContext.get(DataContext.Variable.LOCAL_TIMESTAMP.camelName);

        System.out.println(TestUtils.MockBolt.getCollectedValues());

        java.sql.Timestamp timestamp = new java.sql.Timestamp(utcTimestamp);
        int dateInt = (int) timestamp.toLocalDateTime().atOffset(ZoneOffset.UTC).toLocalDate().toEpochDay();
        int localTimeInt = (int) (localTimestamp % DateTimeUtils.MILLIS_PER_DAY);
        int currentTimeInt = (int) (currentTimestamp % DateTimeUtils.MILLIS_PER_DAY);

        Assert.assertArrayEquals(new Values[]{new Values(localTimeInt, currentTimeInt, localTimestamp, currentTimestamp, dateInt,
            134, 0, 1451606400000L, 1L, 0L, 45300000, true, true, true, 4L, 1475799300000L, 86400, 17, 0, 14)},
            TestUtils.MockBolt.getCollectedValues().toArray());
    }
}
