/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql.compiler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestCalcitePlanSerializer {
  @Test
  @SuppressWarnings("unchecked")
  public void testTableSchema() throws Exception {
    String sql = "SELECT 1 FROM FOO";
    CalciteState state = sqlOverDummyTable(sql);
    String res = new CalcitePlanSerializer(state.schema, state.tree).toJson();

    ObjectMapper m = new ObjectMapper();
    HashMap<String, Object> o = m.readValue(res, HashMap.class);
    List<HashMap<String, Object>> tables = asList(o.get("tables"));
    HashMap<String, Object> t = tables.get(0);
    assertEquals("FOO", t.get("name"));
    List<HashMap<String, Object>> fields = asList(t.get("fields"));
    assertEquals(1, fields.size());
    HashMap<String, Object> field = fields.get(0);
    assertEquals("ID", field.get("name"));
    assertEquals("INTEGER", field.get("type"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLiteral() throws Exception {
    String sql = "SELECT 1,1.0,TRUE,'FOO' FROM FOO";
    CalciteState state = sqlOverDummyTable(sql);
    String res = new CalcitePlanSerializer(state.schema, state.tree).toJson();

    ObjectMapper m = new ObjectMapper();
    HashMap<String, Object> o = m.readValue(res, HashMap.class);
    List<HashMap<String, Object>> stages = asList(o.get("stages"));
    HashMap<String, Object> project = null;
    for (HashMap<String, Object> s : stages) {
      if ("LogicalProject".equals(s.get("type"))) {
        project = s;
        break;
      }
    }
    assertNotNull(project);
    List<HashMap<String, Object>> exprs = asList(project.get("exprs"));
    assertEquals(4, exprs.size());
    checkLiteral("INTEGER", Integer.toString(1), exprs.get(0));
  }

  private static class CalciteState {
    private final SchemaPlus schema;
    private final RelNode tree;

    private CalciteState(SchemaPlus schema, RelNode tree) {
      this.schema = schema;
      this.tree = tree;
    }
  }

  @SuppressWarnings("unchecked")
  private List<HashMap<String, Object>> asList(Object p) {
    return (List<HashMap<String, Object>>) p;
  }

  private CalciteState sqlOverDummyTable(String sql)
      throws RelConversionException, ValidationException, SqlParseException {
    SchemaPlus schema = Frameworks.createRootSchema(true);
    Table table = TestUtils.newTable().field("ID", SqlTypeName.INTEGER).build();
    schema.add("FOO", table);
    FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(
        schema).build();
    Planner planner = Frameworks.getPlanner(config);
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode tree = planner.convert(validate);
    return new CalciteState(schema, tree);
  }

  @SuppressWarnings("unchecked")
  private void checkLiteral(String type, String value, Object l) {
    HashMap<String, Object> e = (HashMap<String, Object>) ((HashMap<String, Object>) l).get(
        "expr");
    assertEquals(type, e.get("type"));
    HashMap<String, Object> v = (HashMap<String, Object>) e.get("value");
    assertEquals("literal", v.get("inst"));
    assertEquals(value, v.get("value"));
  }
}
