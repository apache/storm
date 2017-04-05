/**
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
package org.apache.storm.sql.compiler;

import com.google.common.base.Function;
import org.apache.storm.sql.compiler.backends.standalone.TestCompilerUtils;
import org.apache.storm.tuple.Values;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.storm.sql.TestUtils;
import org.apache.storm.sql.compiler.backends.standalone.PlanCompiler;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.AbstractValuesProcessor;
import org.junit.Test;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestExprSemantic {
  private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(
      RelDataTypeSystem.DEFAULT);

  @Test
  public void testLogicalExpr() throws Exception {
    Values v = testExpr(
        Lists.newArrayList("ID > 0 OR ID < 1", "ID > 0 AND ID < 1",
                           "NOT (ID > 0 AND ID < 1)"));
    assertEquals(new Values(true, false, true), v);
  }

  @Test
  public void testExpectOperator() throws Exception {
    Values v = testExpr(
        Lists.newArrayList("TRUE IS TRUE", "TRUE IS NOT TRUE",
                           "UNKNOWN IS TRUE", "UNKNOWN IS NOT TRUE",
                           "TRUE IS FALSE", "UNKNOWN IS NULL",
                           "UNKNOWN IS NOT NULL"));
    assertEquals(new Values(true, false, false, true, false, true, false), v);
  }

  @Test
  public void testDistinctBetweenLikeSimilarIn() throws Exception {
    Values v = testExpr(
            Lists.newArrayList("TRUE IS DISTINCT FROM TRUE",
                    "TRUE IS NOT DISTINCT FROM FALSE", "3 BETWEEN 1 AND 5",
                    "10 NOT BETWEEN 1 AND 5", "'hello' LIKE '_e%'",
                    "'world' NOT LIKE 'wor%'", "'abc' SIMILAR TO '[a-zA-Z]+[cd]{1}'",
                    "'abe' NOT SIMILAR TO '[a-zA-Z]+[cd]{1}'", "'3' IN ('1', '2', '3', '4')",
                    "2 NOT IN (1, 3, 5)"));
    assertEquals(new Values(false, false, true, true, true,
          false, true, true, true, true), v);
  }

  @Test
  public void testCaseStatement() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "CASE WHEN 'abcd' IN ('a', 'abc', 'abcde') THEN UPPER('a') " +
                    "WHEN UPPER('abcd') = 'AB' THEN 'b' ELSE {fn CONCAT('abcd', '#')} END",
                    "CASE WHEN 'ab' IN ('a', 'abc', 'abcde') THEN UPPER('a') " +
                    "WHEN UPPER('ab') = 'AB' THEN 'b' ELSE {fn CONCAT('ab', '#')} END",
                    "CASE WHEN 'abc' IN ('a', 'abc', 'abcde') THEN UPPER('a') " +
                    "WHEN UPPER('abc') = 'AB' THEN 'b' ELSE {fn CONCAT('abc', '#')} END"
                    )
    );

    // TODO: The data type of literal Calcite assigns seems to be out of expectation. Please see below logical plan.
    // LogicalProject(EXPR$0=[CASE(OR(=('abcd', 'a'), =('abcd', 'abc'), =('abcd', 'abcde')), CAST(UPPER('a')):VARCHAR(5) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary" NOT NULL, =(UPPER('abcd'), CAST('AB'):CHAR(4) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary" NOT NULL), 'b', CAST(||('abcd', '#')):VARCHAR(5) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary" NOT NULL)], EXPR$1=[CASE(OR(=('ab', 'a'), =('ab', 'abc'), =('ab', 'abcde')), CAST(UPPER('a')):CHAR(3) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary" NOT NULL, =(UPPER('ab'), 'AB'), CAST('b'):CHAR(3) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary" NOT NULL, ||('ab', '#'))], EXPR$2=[CASE(OR(=('abc', 'a'), =('abc', 'abc'), =('abc', 'abcde')), CAST(UPPER('a')):CHAR(4) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary" NOT NULL, =(UPPER('abc'), CAST('AB'):CHAR(3) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary" NOT NULL), CAST('b'):CHAR(4) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary" NOT NULL, ||('abc', '#'))]): rowcount = 1.0, cumulative cost = {2.0 rows, 5.0 cpu, 0.0 io}, id = 5
    //   LogicalFilter(condition=[AND(>($0, 0), <($0, 2))]): rowcount = 1.0, cumulative cost = {1.0 rows, 2.0 cpu, 0.0 io}, id = 4
    //     EnumerableTableScan(table=[[FOO]]): rowcount = 1.0, cumulative cost = {0.0 rows, 1.0 cpu, 0.0 io}, id = 3
    // in result, both 'b' and UPPER('a') hence 'A' are having some spaces which is not expected.
    // When we use CASE with actual column (Java String type hence VARCHAR), it seems to work as expected.
    // Please refer trident/TestPlanCompiler#testCaseStatement(), and see below logical plan.
    // LogicalProject(EXPR$0=[CASE(OR(=($1, 'a'), =($1, 'abc'), =($1, 'abcde')), CAST(UPPER('a')):VARCHAR(1) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary", =(CAST(UPPER($1)):VARCHAR(2) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary", 'AB'), 'b', CAST(||($1, '#')):VARCHAR(1) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary")]): rowcount = 1.0, cumulative cost = {1.0 rows, 2.0 cpu, 0.0 io}, id = 3
    List<Object> v2 = Lists.transform(v, new Function<Object, Object>() {
      @Nullable
      @Override
      public String apply(@Nullable Object o) {
        return ((String) o).trim();
      }
    });
    assertArrayEquals(new Values("abcd#", "b", "A").toArray(), v2.toArray());
  }

  @Test
  public void testNullIfAndCoalesce() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "NULLIF(5, 5)", "NULLIF(5, 0)", "COALESCE(NULL, NULL, 5, 4, NULL)", "COALESCE(1, 5)"
            ));
    assertEquals(new Values(null, 5, 5, 1), v);
  }

  @Test
  public void testCollectionFunctions() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "ELEMENT(ARRAY[3])", "CARDINALITY(ARRAY[1, 2, 3, 4, 5])"
            ));
    assertEquals(new Values(3, 5), v);
  }

  @Test(expected = RuntimeException.class)
  public void testElementFunctionMoreThanOneValue() throws Exception {
    testExpr(
            Lists.newArrayList(
                    "ELEMENT(ARRAY[1, 2, 3])"
            ));
    fail("ELEMENT with array which has multiple elements should throw exception in runtime.");
  }

  @Test
  public void testArithmeticWithNull() throws Exception {
    Values v = testExpr(
      Lists.newArrayList(
          "1 + CAST(NULL AS INT)", "CAST(NULL AS INT) + 1", "CAST(NULL AS INT) + CAST(NULL AS INT)", "1 + 2"
      ));
    assertEquals(new Values(null, null, null, 3), v);
  }

  @Test
  public void testNotWithNull() throws Exception {
    Values v = testExpr(
        Lists.newArrayList(
            "NOT TRUE", "NOT FALSE", "NOT UNKNOWN"
        ));
    assertEquals(new Values(false, true, null), v);
  }

  @Test
  public void testAndWithNull() throws Exception {
    Values v = testExpr(
        Lists.newArrayList(
            "UNKNOWN AND TRUE", "UNKNOWN AND FALSE", "UNKNOWN AND UNKNOWN",
            "TRUE AND TRUE", "TRUE AND FALSE", "TRUE AND UNKNOWN",
            "FALSE AND TRUE", "FALSE AND FALSE", "FALSE AND UNKNOWN"
        ));
    assertEquals(new Values(null, false, null, true, false, null, false,
                            false, false), v);
  }

  @Test
  public void testAndWithNullable() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "ADDR = 'a' AND NAME = 'a'", "NAME = 'a' AND ADDR = 'a'", "NAME = 'x' AND ADDR = 'a'", "ADDR = 'a' AND NAME = 'x'"
            ));
    assertEquals(new Values(false, false, null, null), v);
  }

  @Test
  public void testOrWithNullable() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "ADDR = 'a'  OR NAME = 'a'", "NAME = 'a' OR ADDR = 'a' ", "NAME = 'x' OR ADDR = 'a' ", "ADDR = 'a'  OR NAME = 'x'"
            ));
    assertEquals(new Values(null, null, true, true), v);
  }

  @Test
  public void testOrWithNull() throws Exception {
    Values v = testExpr(
        Lists.newArrayList(
            "UNKNOWN OR TRUE", "UNKNOWN OR FALSE", "UNKNOWN OR UNKNOWN",
            "TRUE OR TRUE", "TRUE OR FALSE", "TRUE OR UNKNOWN",
            "FALSE OR TRUE", "FALSE OR FALSE", "FALSE OR UNKNOWN"
            ));
    assertEquals(new Values(true, null, null, true, true, true, true,
                            false, null), v);
  }

  @Test
  public void testEquals() throws Exception {
    Values v = testExpr(
        Lists.newArrayList(
            "1 = 2", "UNKNOWN = UNKNOWN", "'a' = 'a'", "'a' = UNKNOWN", "UNKNOWN = 'a'", "'a' = 'b'",
            "1 <> 2", "UNKNOWN <> UNKNOWN", "'a' <> 'a'", "'a' <> UNKNOWN", "UNKNOWN <> 'a'", "'a' <> 'b'"
        ));
    assertEquals(new Values(false, null, true, null, null, false,
        true, null, false, null, null, true), v);
  }

  @Test
  public void testArithmeticFunctions() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "POWER(3, 2)", "ABS(-10)", "MOD(10, 3)", "MOD(-10, 3)",
                    "CEIL(123.45)", "FLOOR(123.45)"
            ));

    assertEquals(new Values(9.0d, 10, 1, -1, new BigDecimal(124), new BigDecimal(123)), v);

    // Belows are floating numbers so comparing this with literal is tend to be failing...
    // Picking int value and compare
    Values v2 = testExpr(
            Lists.newArrayList(
                    "SQRT(255)", "LN(16)", "LOG10(10000)", "EXP(10)"
            ));
    List<Object> v2m = Lists.transform(v2, new Function<Object, Object>() {
      @Nullable
      @Override
      public Object apply(@Nullable Object o) {
        // only takes int value
        return ((Number) o).intValue();
      }
    });

    // 15.9687, 2.7725, 4.0, 22026.465794
    assertEquals(new Values(15, 2, 4, 22026), v2m);
  }

  @Test
  public void testStringFunctions() throws Exception {
    Values v = testExpr(
        Lists.newArrayList(
                "'ab' || 'cd'", "CHAR_LENGTH('foo')", "CHARACTER_LENGTH('foo')",
                "UPPER('a')", "LOWER('A')", "POSITION('bc' IN 'abcd')",
                "TRIM(BOTH ' ' FROM '  abcdeabcdeabc  ')",
                "TRIM(LEADING ' ' FROM '  abcdeabcdeabc  ')",
                "TRIM(TRAILING ' ' FROM '  abcdeabcdeabc  ')",
                "OVERLAY('abcde' PLACING 'bc' FROM 3)",
                "SUBSTRING('abcde' FROM 3)", "SUBSTRING('abcdeabcde' FROM 3 FOR 4)",
                "INITCAP('foo')"
        ));
    assertEquals(new Values("abcd", 3, 3, "A", "a", 2, "abcdeabcdeabc", "abcdeabcdeabc  ", "  abcdeabcdeabc", "abbce", "cde", "cdea", "Foo"), v);
  }

  @Test
  public void testBinaryStringFunctions() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "x'45F0AB' || x'45F0AB'",
                    "POSITION(x'F0' IN x'453423F0ABBC')",
                    "OVERLAY(x'453423F0ABBC45' PLACING x'4534' FROM 3)"
                    // "SUBSTRING(x'453423F0ABBC' FROM 3)",
                    // "SUBSTRING(x'453423F0ABBC453423F0ABBC' FROM 3 FOR 4)"
            ));

    // TODO: Calcite 1.9.0 has bugs on binary SUBSTRING functions
    // as there's no SqlFunctions.substring(org.apache.calcite.avatica.util.ByteString, ...)
    // commented out testing substring function

    assertEquals("45f0ab45f0ab", v.get(0).toString());
    assertEquals(4, v.get(1));
    assertEquals("45344534abbc45", v.get(2).toString());
    // assertEquals("23f0abbc", v.get(3).toString());
    // assertEquals("23f0ab", v.get(4).toString());
  }

  @Test
  public void testDateAndTimestampLiteral() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "DATE '1970-05-15' AS datefield",
                    "TIME '00:00:00' AS timefield",
                    "TIMESTAMP '2016-01-01 00:00:00' as timestampfield"
            )
    );

    assertEquals(3, v.size());
    assertEquals(134, v.get(0));
    assertEquals(0, v.get(1));
    assertEquals(1451606400000L, v.get(2));
  }

  @Test
  public void testInterval() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "INTERVAL '1-5' YEAR TO MONTH AS intervalfield",
                    "(DATE '1970-01-01', DATE '1970-01-15') AS anchoredinterval_field"
            )
    );

    assertEquals(3, v.size());
    assertEquals(17, v.get(0));
    assertEquals(0, v.get(1));
    assertEquals(14, v.get(2));
  }

  @Test
  public void testDateFunctions() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "LOCALTIME = CURRENT_TIME, LOCALTIMESTAMP = CURRENT_TIMESTAMP, CURRENT_DATE",
                    "EXTRACT(MONTH FROM TIMESTAMP '2010-01-23 12:34:56')",
                    "FLOOR(DATE '2016-01-23' TO MONTH)",
                    "CEIL(TIME '12:34:56' TO MINUTE)"
            )
    );

    assertEquals(6, v.size());
    assertTrue((boolean) v.get(0));
    assertTrue((boolean) v.get(1));
    // skip checking CURRENT_DATE since we don't inject dataContext so don't know about current timestamp
    // we can do it from trident test
    assertEquals(1L, v.get(3));
    assertEquals(0L, v.get(4));
    assertEquals(45300000, v.get(5));
  }

  @Test
  public void testJDBCNumericFunctions() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "{fn POWER(3, 2)}", "{fn ABS(-10)}", "{fn MOD(10, 3)}", "{fn MOD(-10, 3)}"
            ));

    assertEquals(new Values(9.0d, 10, 1, -1), v);

    // Belows are floating numbers so comparing this with literal is tend to be failing...
    // Picking int value and compare
    Values v2 = testExpr(
            Lists.newArrayList(
                    "{fn LOG(16)}", "{fn LOG10(10000)}", "{fn EXP(10)}"
            ));
    List<Object> v2m = Lists.transform(v2, new Function<Object, Object>() {
      @Nullable
      @Override
      public Object apply(@Nullable Object o) {
        // only takes int value
        return ((Number) o).intValue();
      }
    });

    // 2.7725, 4.0, 22026.465794
    assertEquals(new Values(2, 4, 22026), v2m);
  }

  @Test
  public void testJDBCStringFunctions() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "{fn CONCAT('ab', 'cd')}",
                    "{fn LOCATE('bc', 'abcdeabcde')}",
                    //"{fn LOCATE('bc', 'abcdeabcde', 4)}",
                    "{fn INSERT('abcd', 2, 3, 'de')}",
                    "{fn LCASE('AbCdE')}",
                    "{fn LENGTH('AbCdE')}",
                    //"{fn LTRIM('  abcde  ')}",
                    //"{fn RTRIM('  abcde  ')}",
                    "{fn SUBSTRING('abcdeabcde', 3, 4)}",
                    "{fn UCASE('AbCdE')}"
            )
    );

    // TODO: Calcite 1.9.0 doesn't support {fn LOCATE(string1, string2 [, integer])}
    // while it's on support list on SQL reference
    // and bugs on LTRIM and RTRIM : throwing AssertionError: Internal error: pre-condition failed: pos != null
    // commented out problematic function tests

    assertEquals(new Values("abcd", 2, "ade", "abcde", 5, "cdea", "ABCDE"), v);
  }

  @Test
  public void testJDBCDateTimeFunctions() throws Exception {
    Values v = testExpr(
            Lists.newArrayList(
                    "{fn CURDATE()} = CURRENT_DATE", "{fn CURTIME()} = LOCALTIME", "{fn NOW()} = LOCALTIMESTAMP",
                    "{fn QUARTER(DATE '2016-10-07')}", "{fn TIMESTAMPADD(MINUTE, 15, TIMESTAMP '2016-10-07 00:00:00')}",
                    "{fn TIMESTAMPDIFF(SECOND, TIMESTAMP '2016-10-06 00:00:00', TIMESTAMP '2016-10-07 00:00:00')}"
            )
    );

    assertEquals(new Values(true, true, true, 4L, 1475799300000L, 86400), v);
  }

  private Values testExpr(List<String> exprs) throws Exception {
    String sql = "SELECT " + Joiner.on(',').join(exprs) + " FROM FOO" +
        " WHERE ID > 0 AND ID < 2";
    TestCompilerUtils.CalciteState state = TestCompilerUtils.sqlOverDummyTable(sql);
    PlanCompiler compiler = new PlanCompiler(typeFactory);
    AbstractValuesProcessor proc = compiler.compile(state.tree());
    Map<String, DataSource> data = new HashMap<>();
    data.put("FOO", new TestUtils.MockDataSource());
    List<Values> values = new ArrayList<>();
    ChannelHandler h = new TestUtils.CollectDataChannelHandler(values);
    proc.initialize(data, h);
    return values.get(0);
  }

}
