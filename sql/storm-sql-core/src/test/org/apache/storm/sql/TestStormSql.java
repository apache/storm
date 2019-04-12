/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.sql;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.tools.ValidationException;
import org.apache.storm.LocalCluster;
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.DataSourcesRegistry;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.streams.Pair;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.MockBoltExtension.class)
@ExtendWith(TestUtils.MockInsertBoltExtension.class)
public class TestStormSql {

    public static final int WAIT_TIMEOUT_MS = 1000 * 1000;
    public static final int WAIT_TIMEOUT_MS_NO_RECORDS_EXPECTED = 1000 * 10;
    public static final int WAIT_TIMEOUT_MS_ERROR_EXPECTED = 1000;

    private static LocalCluster cluster;

    @BeforeAll
    public static void staticSetup() throws Exception {
        DataSourcesRegistry.providerMap().put("mock", new MockDataSourceProvider());
        DataSourcesRegistry.providerMap().put("mocknested", new MockNestedDataSourceProvider());
        DataSourcesRegistry.providerMap().put("mockgroup", new MockGroupDataSourceProvider());
        DataSourcesRegistry.providerMap().put("mockemp", new MockEmpDataSourceProvider());
        DataSourcesRegistry.providerMap().put("mockdept", new MockDeptDataSourceProvider());

        cluster = new LocalCluster();
    }

    @AfterAll
    public static void staticCleanup() {
        DataSourcesRegistry.providerMap().remove("mock");
        DataSourcesRegistry.providerMap().remove("mocknested");
        DataSourcesRegistry.providerMap().remove("mockgroup");
        DataSourcesRegistry.providerMap().remove("mockemp");
        DataSourcesRegistry.providerMap().remove("mockdept");

        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
        }
    }

    @Test
    public void testExternalDataSource() throws Exception {
        List<String> stmt = new ArrayList<>();
        stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY) LOCATION 'mock:///foo'");
        stmt.add("CREATE EXTERNAL TABLE BAR (ID INT PRIMARY KEY) LOCATION 'mock:///foo'");
        stmt.add("INSERT INTO BAR SELECT STREAM ID + 1 FROM FOO WHERE ID > 2");
        StormSqlLocalClusterImpl impl = new StormSqlLocalClusterImpl();

        List<Pair<Object, Values>> values = TestUtils.MockInsertBolt.getCollectedValues();
        impl.runLocal(cluster, stmt, (__) -> values.size() >= 2, WAIT_TIMEOUT_MS);

        Assert.assertEquals(2, values.size());
        Assert.assertEquals(4, values.get(0).getFirst());
        Assert.assertEquals(5, values.get(1).getFirst());
    }

    @Test
    public void testExternalDataSourceNested() throws Exception {
        List<String> stmt = new ArrayList<>();
        stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
        stmt.add("CREATE EXTERNAL TABLE BAR (ID INT PRIMARY KEY, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
        stmt.add("INSERT INTO BAR SELECT STREAM ID, MAPFIELD['c'], NESTEDMAPFIELD, ARRAYFIELD " +
                 "FROM FOO " +
                 "WHERE CAST(MAPFIELD['b'] AS INTEGER) = 2 AND CAST(ARRAYFIELD[2] AS INTEGER) = 200");
        StormSqlLocalClusterImpl impl = new StormSqlLocalClusterImpl();

        List<Pair<Object, Values>> values = TestUtils.MockInsertBolt.getCollectedValues();;
        impl.runLocal(cluster, stmt, (__) -> values.size() >= 1, WAIT_TIMEOUT_MS);

        Map<String, Integer> map = ImmutableMap.of("b", 2, "c", 4);
        Map<String, Map<String, Integer>> nestedMap = ImmutableMap.of("a", map);
        Assert.assertEquals(2, values.get(0).getFirst());
        Assert.assertEquals(new Values(2, 4, nestedMap, Arrays.asList(100, 200, 300)), values.get(0).getSecond());
    }

    @Test
    public void testExternalNestedNonExistKeyAccess() throws Exception {
        List<String> stmt = new ArrayList<>();
        // this triggers java.lang.RuntimeException: Cannot convert null to int
        stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
        stmt.add("CREATE EXTERNAL TABLE BAR (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
        stmt.add("INSERT INTO BAR SELECT STREAM ID, MAPFIELD, NESTEDMAPFIELD, ARRAYFIELD " +
                 "FROM FOO " +
                 "WHERE CAST(MAPFIELD['a'] AS INTEGER) = 2");
        StormSqlLocalClusterImpl impl = new StormSqlLocalClusterImpl();

        List<Pair<Object, Values>> values = TestUtils.MockInsertBolt.getCollectedValues();
        impl.runLocal(cluster, stmt, (__) -> true, WAIT_TIMEOUT_MS_NO_RECORDS_EXPECTED);

        Assert.assertEquals(0, values.size());
    }

    @Test
    public void testExternalNestedNonExistKeyAccess2() throws Exception {
        List<String> stmt = new ArrayList<>();
        // this triggers java.lang.RuntimeException: Cannot convert null to int
        stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
        stmt.add("CREATE EXTERNAL TABLE BAR (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
        stmt.add("INSERT INTO BAR SELECT STREAM ID, MAPFIELD, NESTEDMAPFIELD, ARRAYFIELD " +
                 "FROM FOO " +
                 "WHERE CAST(NESTEDMAPFIELD['b']['c'] AS INTEGER) = 4");
        StormSqlLocalClusterImpl impl = new StormSqlLocalClusterImpl();

        List<Pair<Object, Values>> values = TestUtils.MockInsertBolt.getCollectedValues();
        impl.runLocal(cluster, stmt, (__) -> true, WAIT_TIMEOUT_MS_NO_RECORDS_EXPECTED);

        Assert.assertEquals(0, values.size());
    }

    @Test
    public void testExternalNestedInvalidAccessStringIndexOnArray() throws Exception {
        List<String> stmt = new ArrayList<>();
        stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
        stmt.add("CREATE EXTERNAL TABLE BAR (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
        stmt.add("INSERT INTO BAR SELECT STREAM ID, MAPFIELD, NESTEDMAPFIELD, ARRAYFIELD " +
                 "FROM FOO " +
                 "WHERE CAST(ARRAYFIELD['a'] AS INTEGER) = 200");
        StormSqlLocalClusterImpl impl = new StormSqlLocalClusterImpl();

        List<Pair<Object, Values>> values = TestUtils.MockInsertBolt.getCollectedValues();
        impl.runLocal(cluster, stmt, (__) -> true, WAIT_TIMEOUT_MS_NO_RECORDS_EXPECTED);

        Assert.assertEquals(0, values.size());
    }

    @Test
    public void testExternalNestedArrayOutOfBoundAccess() throws Exception {
        List<String> stmt = new ArrayList<>();
        stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
        stmt.add("CREATE EXTERNAL TABLE BAR (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
        stmt.add("INSERT INTO BAR SELECT STREAM ID, MAPFIELD, NESTEDMAPFIELD, ARRAYFIELD " +
                 "FROM FOO " +
                 "WHERE CAST(ARRAYFIELD[10] AS INTEGER) = 200");
        StormSqlLocalClusterImpl impl = new StormSqlLocalClusterImpl();

        List<Pair<Object, Values>> values = TestUtils.MockInsertBolt.getCollectedValues();
        impl.runLocal(cluster, stmt, (__) -> true, WAIT_TIMEOUT_MS_NO_RECORDS_EXPECTED);

        Assert.assertEquals(0, values.size());
    }

    @Test
    public void testExternalUdfType() throws Exception {
        List<String> stmt = new ArrayList<>();
        stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, NAME VARCHAR) LOCATION 'mock:///foo'");
        stmt.add("CREATE EXTERNAL TABLE BAR (ID INT) LOCATION 'mock:///foo'");
        stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus'");
        stmt.add("INSERT INTO BAR SELECT STREAM MYPLUS(NAME, 1) FROM FOO WHERE ID = 0");
        StormSqlLocalClusterImpl impl = new StormSqlLocalClusterImpl();

        Assertions.assertThrows(ValidationException.class,
            () -> impl.runLocal(cluster, stmt, (__) -> true, WAIT_TIMEOUT_MS_ERROR_EXPECTED));
    }

    @Test
    public void testExternalUdfType2() throws Exception {
        List<String> stmt = new ArrayList<>();
        // generated code will be not compilable since return type of MYPLUS and type of 'x' are different
        stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, NAME VARCHAR) LOCATION 'mock:///foo'");
        stmt.add("CREATE EXTERNAL TABLE BAR (ID INT) LOCATION 'mock:///foo'");
        stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus'");
        stmt.add("INSERT INTO BAR SELECT STREAM ID FROM FOO WHERE MYPLUS(ID, 1) = 'x'");
        StormSqlLocalClusterImpl impl = new StormSqlLocalClusterImpl();

        Assertions.assertThrows(CompilingClassLoader.CompilerException.class,
            () -> impl.runLocal(cluster, stmt, (__) -> true, WAIT_TIMEOUT_MS_ERROR_EXPECTED));
    }

    @Test
    public void testExternalUdf() throws Exception {
        List<String> stmt = new ArrayList<>();
        stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY) LOCATION 'mock:///foo'");
        stmt.add("CREATE EXTERNAL TABLE BAR (ID INT PRIMARY KEY) LOCATION 'mock:///foo'");
        stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus'");
        stmt.add("INSERT INTO BAR SELECT STREAM MYPLUS(ID, 1) FROM FOO WHERE ID > 2");
        StormSqlLocalClusterImpl impl = new StormSqlLocalClusterImpl();

        List<Pair<Object, Values>> values = TestUtils.MockInsertBolt.getCollectedValues();
        impl.runLocal(cluster, stmt, (__) -> values.size() >= 2, WAIT_TIMEOUT_MS);

        Assert.assertEquals(2, values.size());
        Assert.assertEquals(4, values.get(0).getFirst());
        Assert.assertEquals(5, values.get(1).getFirst());
    }

    @Test
    public void testExternalUdfUsingJar() throws Exception {
        List<String> stmt = new ArrayList<>();
        stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY) LOCATION 'mock:///foo'");
        stmt.add("CREATE EXTERNAL TABLE BAR (ID INT PRIMARY KEY) LOCATION 'mock:///foo'");
        stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus' USING JAR 'foo'");
        stmt.add("INSERT INTO BAR SELECT STREAM MYPLUS(ID, 1) FROM FOO WHERE ID > 2");
        StormSqlLocalClusterImpl impl = new StormSqlLocalClusterImpl();

        Assertions.assertThrows(UnsupportedOperationException.class,
            () -> impl.runLocal(cluster, stmt, (__) -> true, WAIT_TIMEOUT_MS_ERROR_EXPECTED));
    }

    private static class MockDataSourceProvider implements DataSourcesProvider {
        @Override
        public String scheme() {
            return "mock";
        }

        @Override
        public ISqlStreamsDataSource constructStreams(URI uri, String inputFormatClass, String outputFormatClass,
                                                      Properties properties, List<FieldInfo> fields) {
            return new TestUtils.MockSqlStreamsInsertDataSource();
        }
    }

    private static class MockNestedDataSourceProvider implements DataSourcesProvider {
        @Override
        public String scheme() {
            return "mocknested";
        }

        @Override
        public ISqlStreamsDataSource constructStreams(URI uri, String inputFormatClass, String outputFormatClass,
                                                      Properties properties, List<FieldInfo> fields) {
            return new TestUtils.MockSqlStreamsInsertNestedDataSource();
        }
    }

    private static class MockGroupDataSourceProvider implements DataSourcesProvider {
        @Override
        public String scheme() {
            return "mockgroup";
        }

        @Override
        public ISqlStreamsDataSource constructStreams(URI uri, String inputFormatClass, String outputFormatClass,
                                                      Properties properties, List<FieldInfo> fields) {
            return new TestUtils.MockSqlStreamsInsertGroupedDataSource();
        }
    }

    private static class MockEmpDataSourceProvider implements DataSourcesProvider {
        @Override
        public String scheme() {
            return "mockemp";
        }

        @Override
        public ISqlStreamsDataSource constructStreams(URI uri, String inputFormatClass, String outputFormatClass,
                                                      Properties properties, List<FieldInfo> fields) {
            return new TestUtils.MockSqlStreamsInsertJoinDataSourceEmp();
        }
    }

    private static class MockDeptDataSourceProvider implements DataSourcesProvider {
        @Override
        public String scheme() {
            return "mockdept";
        }

        @Override
        public ISqlStreamsDataSource constructStreams(URI uri, String inputFormatClass, String outputFormatClass,
                                                      Properties properties, List<FieldInfo> fields) {
            return new TestUtils.MockSqlStreamsInsertJoinDataSourceDept();
        }
    }
}
