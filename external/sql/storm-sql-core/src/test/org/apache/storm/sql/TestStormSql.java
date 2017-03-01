/**
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
 */
package org.apache.storm.sql;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.tools.ValidationException;
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.DataSourcesRegistry;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.tuple.Values;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestStormSql {
  private static class MockDataSourceProvider implements DataSourcesProvider {
    @Override
    public String scheme() {
      return "mock";
    }

    @Override
    public DataSource construct(
        URI uri, String inputFormatClass, String outputFormatClass,
        List<FieldInfo> fields) {
      return new TestUtils.MockDataSource();
    }

    @Override
    public ISqlTridentDataSource constructTrident(URI uri, String inputFormatClass, String outputFormatClass,
                                                  Properties properties, List<FieldInfo> fields) {
      return new TestUtils.MockSqlTridentDataSource();
    }
  }

  private static class MockNestedDataSourceProvider implements DataSourcesProvider {
    @Override
    public String scheme() {
      return "mocknested";
    }

    @Override
    public DataSource construct(
            URI uri, String inputFormatClass, String outputFormatClass,
            List<FieldInfo> fields) {
      return new TestUtils.MockNestedDataSource();
    }

    @Override
    public ISqlTridentDataSource constructTrident(URI uri, String inputFormatClass, String outputFormatClass,
                                                  Properties properties, List<FieldInfo> fields) {
      return new TestUtils.MockSqlTridentDataSource();
    }
  }

  private static class MockGroupDataSourceProvider implements DataSourcesProvider {
    @Override
    public String scheme() {
      return "mockgroup";
    }

    @Override
    public DataSource construct(
            URI uri, String inputFormatClass, String outputFormatClass,
            List<FieldInfo> fields) {
      return new TestUtils.MockGroupDataSource();
    }

    @Override
    public ISqlTridentDataSource constructTrident(URI uri, String inputFormatClass, String outputFormatClass,
                                                  Properties properties, List<FieldInfo> fields) {
      return new TestUtils.MockSqlTridentGroupedDataSource();
    }
  }

  private static class MockEmpDataSourceProvider implements DataSourcesProvider {
    @Override
    public String scheme() {
      return "mockemp";
    }

    @Override
    public DataSource construct(
            URI uri, String inputFormatClass, String outputFormatClass,
            List<FieldInfo> fields) {
      return new TestUtils.MockEmpDataSource();
    }

    @Override
    public ISqlTridentDataSource constructTrident(URI uri, String inputFormatClass, String outputFormatClass,
                                                  Properties properties, List<FieldInfo> fields) {
      return new TestUtils.MockSqlTridentJoinDataSourceEmp();
    }
  }

  private static class MockDeptDataSourceProvider implements DataSourcesProvider {
    @Override
    public String scheme() {
      return "mockdept";
    }

    @Override
    public DataSource construct(
            URI uri, String inputFormatClass, String outputFormatClass,
            List<FieldInfo> fields) {
      return new TestUtils.MockDeptDataSource();
    }

    @Override
    public ISqlTridentDataSource constructTrident(URI uri, String inputFormatClass, String outputFormatClass,
                                                  Properties properties, List<FieldInfo> fields) {
      return new TestUtils.MockSqlTridentJoinDataSourceDept();
    }
  }


  @BeforeClass
  public static void setUp() {
    DataSourcesRegistry.providerMap().put("mock", new MockDataSourceProvider());
    DataSourcesRegistry.providerMap().put("mocknested", new MockNestedDataSourceProvider());
    DataSourcesRegistry.providerMap().put("mockgroup", new MockGroupDataSourceProvider());
    DataSourcesRegistry.providerMap().put("mockemp", new MockEmpDataSourceProvider());
    DataSourcesRegistry.providerMap().put("mockdept", new MockDeptDataSourceProvider());
  }

  @AfterClass
  public static void tearDown() {
    DataSourcesRegistry.providerMap().remove("mock");
    DataSourcesRegistry.providerMap().remove("mocknested");
  }

  @Test
  public void testExternalDataSource() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT) LOCATION 'mock:///foo'");
    stmt.add("SELECT STREAM ID + 1 FROM FOO WHERE ID > 2");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(2, values.size());
    Assert.assertEquals(4, values.get(0).get(0));
    Assert.assertEquals(5, values.get(1).get(0));
  }

  @Test
  public void testExternalDataSourceNested() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
    stmt.add("SELECT STREAM ID, MAPFIELD['c'], NESTEDMAPFIELD, ARRAYFIELD " +
                     "FROM FOO " +
                     "WHERE CAST(MAPFIELD['b'] AS INTEGER) = 2 AND CAST(ARRAYFIELD[2] AS INTEGER) = 200");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    System.out.println(values);
    Map<String, Integer> map = ImmutableMap.of("b", 2, "c", 4);
    Map<String, Map<String, Integer>> nestedMap = ImmutableMap.of("a", map);
    Assert.assertEquals(new Values(2, 4, nestedMap, Arrays.asList(100, 200, 300)), values.get(0));
  }

  @Test
  public void testExternalNestedNonExistKeyAccess() throws Exception {
    List<String> stmt = new ArrayList<>();
    // this triggers java.lang.RuntimeException: Cannot convert null to int
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
    stmt.add("SELECT STREAM ID, MAPFIELD, NESTEDMAPFIELD, ARRAYFIELD " +
             "FROM FOO " +
             "WHERE CAST(MAPFIELD['a'] AS INTEGER) = 2");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(0, values.size());
  }

  @Test
  public void testExternalNestedNonExistKeyAccess2() throws Exception {
    List<String> stmt = new ArrayList<>();
    // this triggers java.lang.RuntimeException: Cannot convert null to int
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
    stmt.add("SELECT STREAM ID, MAPFIELD, NESTEDMAPFIELD, ARRAYFIELD " +
             "FROM FOO " +
             "WHERE CAST(NESTEDMAPFIELD['b']['c'] AS INTEGER) = 4");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(0, values.size());
  }

  @Test
  public void testExternalNestedInvalidAccessStringIndexOnArray() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
    stmt.add("SELECT STREAM ID, MAPFIELD, NESTEDMAPFIELD, ARRAYFIELD " +
             "FROM FOO " +
             "WHERE CAST(ARRAYFIELD['a'] AS INTEGER) = 200");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(0, values.size());
  }

  @Test
  public void testExternalNestedArrayOutOfBoundAccess() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, MAPFIELD ANY, NESTEDMAPFIELD ANY, ARRAYFIELD ANY) LOCATION 'mocknested:///foo'");
    stmt.add("SELECT STREAM ID, MAPFIELD, NESTEDMAPFIELD, ARRAYFIELD " +
             "FROM FOO " +
             "WHERE CAST(ARRAYFIELD[10] AS INTEGER) = 200");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(0, values.size());
  }

  @Test(expected = ValidationException.class)
  public void testExternalUdfType() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, NAME VARCHAR) LOCATION 'mock:///foo'");
    stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus'");
    stmt.add("SELECT STREAM MYPLUS(NAME, 1) FROM FOO WHERE ID = 0");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    System.out.println(values);

  }

  @Test(expected = CompilingClassLoader.CompilerException.class)
  public void testExternalUdfType2() throws Exception {
    List<String> stmt = new ArrayList<>();
    // generated code will be not compilable since return type of MYPLUS and type of 'x' are different
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT, NAME VARCHAR) LOCATION 'mock:///foo'");
    stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus'");
    stmt.add("SELECT STREAM ID FROM FOO WHERE MYPLUS(ID, 1) = 'x'");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(0, values.size());
  }

  @Test
  public void testExternalUdf() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT) LOCATION 'mock:///foo'");
    stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus'");
    stmt.add("SELECT STREAM MYPLUS(ID, 1) FROM FOO WHERE ID > 2");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(2, values.size());
    Assert.assertEquals(4, values.get(0).get(0));
    Assert.assertEquals(5, values.get(1).get(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testExternalUdfUsingJar() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT) LOCATION 'mock:///foo'");
    stmt.add("CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus' USING JAR 'foo'");
    stmt.add("SELECT STREAM MYPLUS(ID, 1) FROM FOO WHERE ID > 2");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
  }

  @Test
  public void testGroupbyBuiltin() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY, SALARY INT, PCT DOUBLE, NAME VARCHAR) LOCATION 'mockgroup:///foo'");
    stmt.add("SELECT STREAM ID, COUNT(*), SUM(SALARY), AVG(SALARY) FROM FOO GROUP BY (ID)");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(4, values.size());
    Assert.assertEquals(3, values.get(0).get(2));
    Assert.assertEquals(12, values.get(1).get(2));
    Assert.assertEquals(21, values.get(2).get(2));
    Assert.assertEquals(9, values.get(3).get(2));
  }

  @Test
  public void testGroupbyBuiltinWithFilter() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY, SALARY INT, PCT DOUBLE, NAME VARCHAR) LOCATION 'mockgroup:///foo'");
    stmt.add("SELECT STREAM ID, COUNT(*), SUM(SALARY), AVG(PCT) FROM FOO WHERE ID = 1 GROUP BY (ID)");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(1, values.get(0).get(0));
    Assert.assertEquals(3L, values.get(0).get(1));
    Assert.assertEquals(12, values.get(0).get(2));
    Assert.assertEquals(2.5, values.get(0).get(3));
  }

  @Test
  public void testGroupbyBuiltinAndUDF() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY, SALARY INT, PCT DOUBLE, NAME VARCHAR) LOCATION 'mockgroup:///foo'");
    stmt.add("CREATE FUNCTION MYCONCAT AS 'org.apache.storm.sql.TestUtils$MyConcat'");
    stmt.add("CREATE FUNCTION TOPN AS 'org.apache.storm.sql.TestUtils$TopN'");
    stmt.add("SELECT STREAM ID, SUM(SALARY), MYCONCAT(NAME), TOPN(2, SALARY) FROM FOO GROUP BY (ID)");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(4, values.size());
    Assert.assertEquals(3, values.get(0).get(1));
    Assert.assertEquals("xxx", values.get(0).get(2));
    Assert.assertEquals(Arrays.asList(2, 1), values.get(0).get(3));
    Assert.assertEquals(12, values.get(1).get(1));
    Assert.assertEquals("xxx", values.get(1).get(2));
    Assert.assertEquals(Arrays.asList(5, 4), values.get(1).get(3));
    Assert.assertEquals(21, values.get(2).get(1));
    Assert.assertEquals("xxx", values.get(2).get(2));
    Assert.assertEquals(Arrays.asList(8, 7), values.get(2).get(3));
    Assert.assertEquals(9, values.get(3).get(1));
    Assert.assertEquals("x", values.get(3).get(2));
    Assert.assertEquals(Arrays.asList(9), values.get(3).get(3));
  }

  @Test
  public void testAggFnNonSqlReturnType() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY, SALARY INT, PCT DOUBLE, NAME VARCHAR) LOCATION 'mockgroup:///foo'");
    stmt.add("CREATE FUNCTION TOPN AS 'org.apache.storm.sql.TestUtils$TopN'");
    stmt.add("SELECT STREAM ID, SUM(SALARY), TOPN(1, SALARY) FROM FOO WHERE ID >= 0 GROUP BY (ID) HAVING MAX(SALARY) > 0");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(4, values.size());
    Assert.assertEquals(Collections.singletonList(2), values.get(0).get(2));
    Assert.assertEquals(Collections.singletonList(5), values.get(1).get(2));
    Assert.assertEquals(Collections.singletonList(8), values.get(2).get(2));
    Assert.assertEquals(Collections.singletonList(9), values.get(3).get(2));
  }

  @Test
  public void testGroupbySameAggregateOnDifferentColumns() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY, SALARY INT, PCT DOUBLE, NAME VARCHAR) LOCATION 'mockgroup:///foo'");
    stmt.add("SELECT STREAM ID, COUNT(*), AVG(SALARY), AVG(PCT) FROM FOO WHERE ID = 1 GROUP BY (ID)");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(1, values.get(0).get(0));
    Assert.assertEquals(3L, values.get(0).get(1));
    Assert.assertEquals(4, values.get(0).get(2));
    Assert.assertEquals(2.5, values.get(0).get(3));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGroupbyBuiltinNotimplemented() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY, SALARY INT, PCT DOUBLE, NAME VARCHAR) LOCATION 'mockgroup:///foo'");
    stmt.add("SELECT STREAM ID, COUNT(*), STDDEV_POP(SALARY) FROM FOO GROUP BY (ID)");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
  }

  @Test
  public void testMinMax() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY, SALARY INT, PCT DOUBLE, NAME VARCHAR) LOCATION 'mockgroup:///foo'");
    stmt.add("SELECT STREAM ID, COUNT(*), MIN(SALARY), MAX(PCT) FROM FOO GROUP BY (ID)");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(4, values.size());
    Assert.assertEquals(0, values.get(0).get(2));
    Assert.assertEquals(3, values.get(1).get(2));
    Assert.assertEquals(6, values.get(2).get(2));
    Assert.assertEquals(9, values.get(3).get(2));

    Assert.assertEquals(1.5, values.get(0).get(3));
    Assert.assertEquals(3.0, values.get(1).get(3));
    Assert.assertEquals(4.5, values.get(2).get(3));
    Assert.assertEquals(5.0, values.get(3).get(3));
  }
  @Test
  public void testFilterGroupbyHaving() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY, SALARY INT, PCT DOUBLE, NAME VARCHAR) LOCATION 'mockgroup:///foo'");
    stmt.add("SELECT STREAM ID, MIN(SALARY) FROM FOO where ID > 0 GROUP BY (ID) HAVING ID > 2 AND MAX(SALARY) > 5");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(3, values.get(0).get(0));
    Assert.assertEquals(9, values.get(0).get(1));
  }

  @Test
  public void testGroupByMultipleFields() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE FOO (DEPTID INT PRIMARY KEY, SALARY INT, PCT DOUBLE, NAME VARCHAR, EMPID INT) LOCATION 'mockgroup:///foo'");
    stmt.add("SELECT STREAM DEPTID, EMPID, COUNT(*), MIN(SALARY), MAX(PCT) FROM FOO GROUP BY DEPTID, EMPID");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(7, values.size());
    Assert.assertEquals(0, values.get(0).get(0));
    Assert.assertEquals(0, values.get(0).get(1));
    Assert.assertEquals(2L, values.get(0).get(2));
  }

  @Test
  public void testjoin() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE EMP (EMPID INT PRIMARY KEY, EMPNAME VARCHAR, DEPTID INT) LOCATION 'mockemp:///foo'");
    stmt.add("CREATE EXTERNAL TABLE DEPT (DEPTID INT PRIMARY KEY, DEPTNAME VARCHAR) LOCATION 'mockdept:///foo'");
    stmt.add("SELECT STREAM EMPID, EMPNAME, DEPTNAME FROM EMP AS e JOIN DEPT AS d ON e.DEPTID = d.DEPTID WHERE e.empid > 0");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    System.out.println(values);
    Assert.assertEquals(3, values.size());
    Assert.assertEquals("emp1", values.get(0).get(1));
    Assert.assertEquals("dept1", values.get(0).get(2));
    Assert.assertEquals("emp2", values.get(1).get(1));
    Assert.assertEquals("dept1", values.get(1).get(2));
    Assert.assertEquals("emp3", values.get(2).get(1));
    Assert.assertEquals("dept2", values.get(2).get(2));
  }

  @Test
  public void testjoinAndGroupby() throws Exception {
    List<String> stmt = new ArrayList<>();
    stmt.add("CREATE EXTERNAL TABLE EMP (EMPID INT PRIMARY KEY, EMPNAME VARCHAR, DEPTID INT) LOCATION 'mockemp:///foo'");
    stmt.add("CREATE EXTERNAL TABLE DEPT (DEPTID INT PRIMARY KEY, DEPTNAME VARCHAR) LOCATION 'mockdept:///foo'");
    stmt.add("SELECT STREAM d.DEPTID, count(EMPID) FROM EMP AS e JOIN DEPT AS d ON e.DEPTID = d.DEPTID WHERE e.empid > 0" +
                     "GROUP BY d.DEPTID");
    StormSql sql = StormSql.construct();
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    sql.execute(stmt, h);
    Assert.assertEquals(2, values.size());
    Assert.assertEquals(1, values.get(0).get(0));
    Assert.assertEquals(2L, values.get(0).get(1));
    Assert.assertEquals(2, values.get(1).get(0));
    Assert.assertEquals(1L, values.get(1).get(1));
  }
}
