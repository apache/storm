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

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitor;

import java.io.IOException;

class ExprSerializer {
  private final TypeSerializer typeSerializer;

  public ExprSerializer(TypeSerializer typeSerializer) {
    this.typeSerializer = typeSerializer;
  }

  public void serialize(JsonGenerator jg, RexNode n) throws IOException {
    new Serializer(jg).serialize(n);
  }

  private class Serializer implements RexVisitor<Void> {
    private final JsonGenerator jg;

    private Serializer(JsonGenerator jg) {
      this.jg = jg;
    }

    private void serialize(RexNode n) throws IOException {
      jg.writeStartObject();
      jg.writeFieldName("type");
      typeSerializer.serialize(jg, n.getType());
      jg.writeObjectFieldStart("value");
      n.accept(this);
      jg.writeEndObject();
      jg.writeEndObject();
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Void visitLocalRef(RexLocalRef localRef) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Void visitLiteral(RexLiteral literal) {
      try {
        jg.writeStringField("inst", "literal");
        jg.writeStringField("value", literal.getValue().toString());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    }

    @Override
    public Void visitCall(RexCall call) {
      try {
        jg.writeStringField("inst", "call");
        jg.writeStringField("operator", call.getOperator().getName());
        jg.writeArrayFieldStart("operands");
        for (RexNode operand : call.getOperands()) {
          serialize(operand);
        }
        jg.writeEndArray();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    }

    @Override
    public Void visitOver(RexOver over) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Void visitCorrelVariable(
        RexCorrelVariable correlVariable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Void visitDynamicParam(
        RexDynamicParam dynamicParam) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Void visitRangeRef(RexRangeRef rangeRef) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      throw new UnsupportedOperationException();
    }
  }
}
