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

package org.apache.storm.sql.compiler.backends.trident;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Test;

/**
 * Only for testing expression. We're leveraging Avatica to make tests faster.
 */
public class TestExpressions {
    @Test
    public void testLogicalExpr() throws Exception {
        List<Object> v = testExpr(
                Lists.newArrayList("s.\"id\" > 0 OR s.\"id\" < 1", "s.\"id\" > 0 AND s.\"id\" < 1",
                        "NOT (s.\"id\" > 0 AND s.\"id\" < 1)"));
        assertEquals(new Values(true, false, true), v);
    }

    @Test
    public void testExpectOperator() throws Exception {
        List<Object> v = testExpr(
                Lists.newArrayList("TRUE IS TRUE", "TRUE IS NOT TRUE",
                        "UNKNOWN IS TRUE", "UNKNOWN IS NOT TRUE",
                        "TRUE IS FALSE", "UNKNOWN IS NULL",
                        "UNKNOWN IS NOT NULL"));
        assertEquals(new Values(true, false, false, true, false, true, false), v);
    }

    @Test
    public void testDistinctBetweenLikeSimilarIn() throws Exception {
        List<Object> v = testExpr(
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
        List<Object> v = testExpr(
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
        List<Object> v = testExpr(
                Lists.newArrayList(
                        "NULLIF(5, 5)", "NULLIF(5, 0)", "COALESCE(NULL, NULL, 5, 4, NULL)", "COALESCE(1, 5)"
                ));
        assertEquals(new Values(null, 5, 5, 1), v);
    }

    @Test
    public void testCollectionFunctions() throws Exception {
        List<Object> v = testExpr(
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
        List<Object> v = testExpr(
                Lists.newArrayList(
                        "1 + CAST(NULL AS INT)", "CAST(NULL AS INT) + 1", "CAST(NULL AS INT) + CAST(NULL AS INT)", "1 + 2"
                ));
        assertEquals(new Values(null, null, null, 3), v);
    }

    @Test
    public void testNotWithNull() throws Exception {
        List<Object> v = testExpr(
                Lists.newArrayList(
                        "NOT TRUE", "NOT FALSE", "NOT UNKNOWN"
                ));
        assertEquals(new Values(false, true, null), v);
    }

    @Test
    public void testAndWithNull() throws Exception {
        List<Object> v = testExpr(
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
        List<Object> v = testExpr(
                Lists.newArrayList(
                        "s.\"addr\" = 'a' AND s.\"name\" = 'a'", "s.\"name\" = 'a' AND s.\"addr\" = 'a'", "s.\"name\" = 'x' AND s.\"addr\" = 'a'", "s.\"addr\" = 'a' AND s.\"name\" = 'x'"
                ));
        assertEquals(new Values(false, false, null, null), v);
    }

    @Test
    public void testOrWithNullable() throws Exception {
        List<Object> v = testExpr(
                Lists.newArrayList(
                        "s.\"addr\" = 'a'  OR s.\"name\" = 'a'", "s.\"name\" = 'a' OR s.\"addr\" = 'a' ", "s.\"name\" = 'x' OR s.\"addr\" = 'a' ", "s.\"addr\" = 'a'  OR s.\"name\" = 'x'"
                ));
        assertEquals(new Values(null, null, true, true), v);
    }

    @Test
    public void testOrWithNull() throws Exception {
        List<Object> v = testExpr(
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
        List<Object> v = testExpr(
                Lists.newArrayList(
                        "1 = 2", "UNKNOWN = UNKNOWN", "'a' = 'a'", "'a' = UNKNOWN", "UNKNOWN = 'a'", "'a' = 'b'",
                        "1 <> 2", "UNKNOWN <> UNKNOWN", "'a' <> 'a'", "'a' <> UNKNOWN", "UNKNOWN <> 'a'", "'a' <> 'b'"
                ));
        assertEquals(new Values(false, null, true, null, null, false,
                true, null, false, null, null, true), v);
    }

    @Test
    public void testArithmeticFunctions() throws Exception {
        List<Object> v = testExpr(
                Lists.newArrayList(
                        "POWER(3, 2)", "ABS(-10)", "MOD(10, 3)", "MOD(-10, 3)",
                        "CEIL(123.45)", "FLOOR(123.45)"
                ));

        assertEquals(new Values(9.0d, 10, 1, -1, new BigDecimal(124), new BigDecimal(123)), v);

        // Belows are floating numbers so comparing this with literal is tend to be failing...
        // Picking int value and compare
        List<Object> v2 = testExpr(
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
        List<Object> v = testExpr(
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
    public void testJDBCNumericFunctions() throws Exception {
        List<Object> v = testExpr(
                Lists.newArrayList(
                        "{fn POWER(3, 2)}", "{fn ABS(-10)}", "{fn MOD(10, 3)}", "{fn MOD(-10, 3)}"
                ));

        assertEquals(new Values(9.0d, 10, 1, -1), v);

        // Belows are floating numbers so comparing this with literal is tend to be failing...
        // Picking int value and compare
        List<Object> v2 = testExpr(
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
        List<Object> v = testExpr(
                Lists.newArrayList(
                        "{fn CONCAT('ab', 'cd')}",
                        "{fn LOCATE('bc', 'abcdeabcde')}",
                        "{fn LOCATE('bc', 'abcdeabcde', 4)}",
                        "{fn INSERT('abcd', 2, 3, 'de')}",
                        "{fn LCASE('AbCdE')}",
                        "{fn LENGTH('AbCdE')}",
                        "{fn LTRIM('  abcde  ')}",
                        "{fn RTRIM('  abcde  ')}",
                        "{fn SUBSTRING('abcdeabcde', 3, 4)}",
                        "{fn UCASE('AbCdE')}"
                )
        );

        assertEquals(new Values("abcd", 2, 7, "ade", "abcde", 5, "abcde  ", "  abcde", "cdea", "ABCDE"), v);
    }

    private List<Object> testExpr(List<String> exprs) throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("expr", new ReflectiveSchema(new ExprDatabase()));
        Statement statement = connection.createStatement();
        ResultSet resultSet =
                statement.executeQuery("select " + Joiner.on(',').join(exprs) + " \n"
                        + "from \"expr\".\"expressions\" as s\n"
                        + " WHERE s.\"id\" > 0 AND s.\"id\" < 2");

        List<Object> result = null;
        while (resultSet.next()) {
            if (result != null) {
                Assert.fail("The query is expected to have only one result row");
            }

            int n = resultSet.getMetaData().getColumnCount();

            result = new ArrayList<>();
            for (int i = 1 ; i <= n; i++) {
                result.add(resultSet.getObject(i));
            }
        }

        resultSet.close();
        statement.close();
        connection.close();

        return result;
    }

    public static class ExprDatabase {
        public final ExpressionTable[] expressions = {
            new ExpressionTable(0, "x", null),
            new ExpressionTable(1, "x", null),
            new ExpressionTable(2, "x", null),
            new ExpressionTable(3, "x", null),
            new ExpressionTable(4, "x", null)
        };
    }

    public static class ExpressionTable {
        public final int id;
        public final String name;
        public final String addr;

        public ExpressionTable(int id, String name, String addr) {
            this.id = id;
            this.name = name;
            this.addr = addr;
        }
    }

}
