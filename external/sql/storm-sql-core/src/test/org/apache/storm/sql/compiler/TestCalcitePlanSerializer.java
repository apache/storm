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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestCalcitePlanSerializer {
  @Test
  @SuppressWarnings("unchecked")
  public void testSerializeTableSchema() throws Exception {
    SchemaPlus schema = Frameworks.createRootSchema(true);
    Table table = TestUtils.newTable().field("ID", SqlTypeName.INTEGER).build();
    schema.add("FOO", table);
    FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(
        schema).build();
    Planner planner = Frameworks.getPlanner(config);
    String sql = "SELECT 1 + 2 FROM FOO WHERE 5 > 3";
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode tree = planner.convert(validate);
    String res = new CalcitePlanSerializer(schema, tree).toJson();

    ObjectMapper m = new ObjectMapper();
    HashMap<String, Object> o = m.readValue(res, HashMap.class);
    List<HashMap<String, Object>> tables = (List<HashMap<String, Object>>) o.get(
        "tables");
    HashMap<String, Object> t = tables.get(0);
    assertEquals("FOO", t.get("name"));
    List<HashMap<String, Object>> fields = (List<HashMap<String, Object>>) t.get(
        "fields");
    assertEquals(1, fields.size());
    HashMap<String, Object> field = fields.get(0);
    assertEquals("ID", field.get("name"));
    assertEquals("INTEGER", field.get("type"));
  }
}
