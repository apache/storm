/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql.compiler;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Compiles a scalar expression ({@link org.apache.calcite.rex.RexNode}) to expression ({@link org.apache.calcite.linq4j.tree.Expression}).
 *
 * This code is inspired by JaninoRexCompiler in Calcite, but while it is returning {@link org.apache.calcite.interpreter.Scalar} which is executable,
 * we need to pass the source code to EvaluationFilter or EvaluationFunction so that they can be serialized, and
 * compiled and executed on worker.
 */
public class RexNodeToBlockStatementCompiler {
  private final RexBuilder rexBuilder;

  public RexNodeToBlockStatementCompiler(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
  }

  public BlockStatement compile(List<RexNode> nodes, RelDataType inputRowType) {
    final RexProgramBuilder programBuilder =
        new RexProgramBuilder(inputRowType, rexBuilder);
    for (RexNode node : nodes) {
      programBuilder.addProject(node, null);
    }
    final RexProgram program = programBuilder.getProgram();

    final BlockBuilder builder = new BlockBuilder();
    final ParameterExpression context_ =
            Expressions.parameter(Context.class, "context");
    final ParameterExpression outputValues_ =
        Expressions.parameter(Object[].class, "outputValues");
    final JavaTypeFactoryImpl javaTypeFactory =
        new JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem());

    final RexToLixTranslator.InputGetter inputGetter =
            new RexToLixTranslator.InputGetterImpl(
                    ImmutableList.of(
                            Pair.<Expression, PhysType>of(
                                    Expressions.field(context_,
                                            BuiltInMethod.CONTEXT_VALUES.field),
                                    PhysTypeImpl.of(javaTypeFactory, inputRowType,
                                            JavaRowFormat.ARRAY, false))));
    final Function1<String, RexToLixTranslator.InputGetter> correlates =
            new Function1<String, RexToLixTranslator.InputGetter>() {
              public RexToLixTranslator.InputGetter apply(String a0) {
                throw new UnsupportedOperationException();
              }
            };
    final Expression root =
            Expressions.field(context_, BuiltInMethod.CONTEXT_ROOT.field);
    final List<Expression> list =
            RexToLixTranslator.translateProjects(program, javaTypeFactory, builder,
                    null, root, inputGetter, correlates);
    for (int i = 0; i < list.size(); i++) {
      builder.add(
              Expressions.statement(
                      Expressions.assign(
                              Expressions.arrayIndex(outputValues_,
                                      Expressions.constant(i)),
                              list.get(i))));
    }

    return builder.toBlock();
  }
}