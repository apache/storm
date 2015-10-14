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
    TestUtils.CalciteState state = TestUtils.sqlOverDummyTable(sql);
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
    TestUtils.CalciteState state = TestUtils.sqlOverDummyTable(sql);
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

  @Test
  @SuppressWarnings("unchecked")
  public void testInputRef() throws Exception {
    String sql = "SELECT ID FROM FOO";
    TestUtils.CalciteState state = TestUtils.sqlOverDummyTable(sql);
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
    assertEquals(1, exprs.size());
    checkInputRef("INTEGER", 0, exprs.get(0));
  }

  @SuppressWarnings("unchecked")
  private List<HashMap<String, Object>> asList(Object p) {
    return (List<HashMap<String, Object>>) p;
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

  @SuppressWarnings("unchecked")
  private void checkInputRef(String type, int idx, Object l) {
    HashMap<String, Object> e = (HashMap<String, Object>) ((HashMap<String, Object>) l).get(
        "expr");
    assertEquals(type, e.get("type"));
    HashMap<String, Object> v = (HashMap<String, Object>) e.get("value");
    assertEquals("inputref", v.get("inst"));
    assertEquals(idx, v.get("idx"));
  }

}
