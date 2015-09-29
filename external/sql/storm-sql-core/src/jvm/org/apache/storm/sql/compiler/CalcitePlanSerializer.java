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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Joiner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Pair;

import java.io.IOException;
import java.io.StringWriter;

public class CalcitePlanSerializer extends PostOrderRelNodeVisitor<Void> {
  private final TypeSerializer typeSerializer = new TypeSerializer();
  private final ExprSerializer exprSerializer = new ExprSerializer(
      typeSerializer);
  private final RelDataTypeFactory sqlTypeFactory = new SqlTypeFactoryImpl(
      RelDataTypeSystem.DEFAULT);

  private final SchemaPlus schema;
  private final RelNode tree;
  private JsonGenerator jg;

  public CalcitePlanSerializer(SchemaPlus schema, RelNode tree) {
    this.schema = schema;
    this.tree = tree;
  }

  public String toJson() throws Exception {
    StringWriter sw = new StringWriter();
    JsonFactory jsonFactory = new JsonFactory();
    try {
      jg = jsonFactory.createGenerator(sw);
      jg.writeStartObject();
      jg.writeNumberField("result", tree.getId());
      jg.writeArrayFieldStart("tables");
      serializeDependency(tree);
      jg.writeEndArray();
      jg.writeArrayFieldStart("stages");
      traverse(tree);
      jg.writeEndArray();
      jg.writeEndObject();
    } finally {
      jg.close();
    }
    return sw.toString();
  }

  private void serializeDependency(RelNode root)
      throws IOException {
    if (root == null) {
      return;
    }
    for (RelNode n : root.getInputs()) {
      serializeDependency(n);
    }
    if (root instanceof TableScan) {
      jg.writeStartObject();
      String name = Joiner.on('.').join(root.getTable().getQualifiedName());
      jg.writeStringField("name", name);
      jg.writeArrayFieldStart("fields");
      Table t = schema.getTable(name);
      RelDataType type = t.getRowType(sqlTypeFactory);
      for (RelDataTypeField field : type.getFieldList()) {
        jg.writeStartObject();
        jg.writeStringField("name", field.getName());
        jg.writeFieldName("type");
        typeSerializer.serialize(jg, field.getType());
        jg.writeEndObject();
      }
      jg.writeEndArray();
      jg.writeEndObject();
    }
  }

  @Override
  Void defaultValue(RelNode n) {
    throw new UnsupportedOperationException("Unsupported RelNode: " + n
        .getClass().getSimpleName());
  }

  @Override
  Void visitFilter(Filter filter) throws Exception {
    jg.writeStartObject();
    writeCommonFieldsForRelNode(jg, filter);
    jg.writeFieldName("expr");
    exprSerializer.serialize(jg, filter.getCondition());
    jg.writeEndObject();
    return null;
  }

  @Override
  Void visitProject(Project project) throws Exception {
    jg.writeStartObject();
    writeCommonFieldsForRelNode(jg, project);
    jg.writeArrayFieldStart("exprs");
    for (Pair<RexNode, String> p : project.getNamedProjects()) {
      jg.writeStartObject();
      jg.writeStringField("name", p.getValue());
      jg.writeFieldName("expr");
      exprSerializer.serialize(jg, p.getKey());
      jg.writeEndObject();
    }
    jg.writeEndArray();
    jg.writeEndObject();
    return null;
  }

  @Override
  Void visitTableScan(TableScan scan) throws Exception {
    jg.writeStartObject();
    writeCommonFieldsForRelNode(jg, scan);
    jg.writeStringField("table",
                        Joiner.on(",").join(scan.getTable().getQualifiedName()));
    jg.writeEndObject();
    return null;
  }

  private void writeCommonFieldsForRelNode(
      JsonGenerator jg, RelNode n) throws IOException {
    jg.writeStringField("type", n.getClass().getSimpleName());
    jg.writeNumberField("id", n.getId());
    jg.writeArrayFieldStart("inputs");
    for (RelNode input : n.getInputs()) {
      jg.writeNumber(input.getId());
    }
    jg.writeEndArray();
  }
}
