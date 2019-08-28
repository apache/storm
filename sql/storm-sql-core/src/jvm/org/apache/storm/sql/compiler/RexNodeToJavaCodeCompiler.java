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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.storm.sql.runtime.calcite.ExecutableExpression;

/**
 * Compiles a scalar expression ({@link org.apache.calcite.rex.RexNode}) to Java source code String.
 *
 * <p>This code is inspired by JaninoRexCompiler in Calcite, but while it is returning
 * {@link org.apache.calcite.interpreter.Scalar} which is executable, we need to pass the source code to compile and
 * serialize instance so that it can be executed on worker efficiently.
 */
public class RexNodeToJavaCodeCompiler {
    private final RexBuilder rexBuilder;

    public RexNodeToJavaCodeCompiler(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    /**
     * Given a method that implements {@link ExecutableExpression#execute(Context, Object[])}, adds a bridge method that
     * implements {@link ExecutableExpression#execute(Context)}, and compiles.
     */
    static String baz(ParameterExpression context,
                      ParameterExpression outputValues, BlockStatement block, String className) {
        final List<MemberDeclaration> declarations = Lists.newArrayList();

        // public void execute(Context, Object[] outputValues)
        declarations.add(
            Expressions.methodDecl(Modifier.PUBLIC, void.class,
                                   StormBuiltInMethod.EXPR_EXECUTE2.method.getName(),
                                   ImmutableList.of(context, outputValues), block));

        // public Object execute(Context)
        final BlockBuilder builder = new BlockBuilder();
        final Expression values_ = builder.append("values",
                                                  Expressions.newArrayBounds(Object.class, 1,
                                                                             Expressions.constant(1)));
        builder.add(
            Expressions.statement(
                Expressions.call(
                    Expressions.parameter(ExecutableExpression.class, "this"),
                    StormBuiltInMethod.EXPR_EXECUTE2.method, context, values_)));
        builder.add(
            Expressions.return_(null,
                                Expressions.arrayIndex(values_, Expressions.constant(0))));
        declarations.add(
            Expressions.methodDecl(Modifier.PUBLIC, Object.class,
                                   StormBuiltInMethod.EXPR_EXECUTE1.method.getName(),
                                   ImmutableList.of(context), builder.toBlock()));

        final ClassDeclaration classDeclaration =
            Expressions.classDecl(Modifier.PUBLIC, className, null,
                                  ImmutableList.<Type>of(ExecutableExpression.class), declarations);

        return Expressions.toString(Lists.newArrayList(classDeclaration), "\n", false);
    }

    public BlockStatement compileToBlock(List<RexNode> nodes, RelDataType inputRowType) {
        final RexProgramBuilder programBuilder =
            new RexProgramBuilder(inputRowType, rexBuilder);
        for (RexNode node : nodes) {
            programBuilder.addProject(node, null);
        }

        return compileToBlock(programBuilder.getProgram());
    }

    public BlockStatement compileToBlock(final RexProgram program) {
        final ParameterExpression context_ =
            Expressions.parameter(Context.class, "context");
        final ParameterExpression outputValues_ =
            Expressions.parameter(Object[].class, "outputValues");

        return compileToBlock(program, context_, outputValues_).toBlock();
    }

    private BlockBuilder compileToBlock(final RexProgram program, ParameterExpression context,
            ParameterExpression outputValues) {
        RelDataType inputRowType = program.getInputRowType();
        final BlockBuilder builder = new BlockBuilder();
        final JavaTypeFactoryImpl javaTypeFactory =
                new JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem());

        final RexToLixTranslator.InputGetter inputGetter =
                new RexToLixTranslator.InputGetterImpl(
                        ImmutableList.of(
                                Pair.<Expression, PhysType>of(
                                        Expressions.field(context,
                                                BuiltInMethod.CONTEXT_VALUES.field),
                                        PhysTypeImpl.of(javaTypeFactory, inputRowType,
                                                JavaRowFormat.ARRAY, false))));
        final Function1<String, RexToLixTranslator.InputGetter> correlates =
            new Function1<String, RexToLixTranslator.InputGetter>() {
                @Override
                public RexToLixTranslator.InputGetter apply(String a0) {
                    throw new UnsupportedOperationException();
                }
            };
        final Expression root =
                Expressions.field(context, BuiltInMethod.CONTEXT_ROOT.field);
        final List<Expression> list =
                RexToLixTranslator.translateProjects(program, javaTypeFactory, builder,
                        null, root, inputGetter, correlates);
        for (int i = 0; i < list.size(); i++) {
            builder.add(
                    Expressions.statement(
                            Expressions.assign(
                                    Expressions.arrayIndex(outputValues,
                                            Expressions.constant(i)),
                                    list.get(i))));
        }

        return builder;
    }

    public String compile(List<RexNode> nodes, RelDataType inputRowType, String className) {
        final RexProgramBuilder programBuilder =
            new RexProgramBuilder(inputRowType, rexBuilder);
        for (RexNode node : nodes) {
            programBuilder.addProject(node, null);
        }

        return compile(programBuilder.getProgram(), className);
    }

    public String compile(final RexProgram program, String className) {
        final ParameterExpression context_ =
            Expressions.parameter(Context.class, "context");
        final ParameterExpression outputValues_ =
            Expressions.parameter(Object[].class, "outputValues");

        BlockBuilder builder = compileToBlock(program, context_, outputValues_);
        return baz(context_, outputValues_, builder.toBlock(), className);
    }

    enum StormBuiltInMethod {
        EXPR_EXECUTE1(ExecutableExpression.class, "execute", Context.class),
        EXPR_EXECUTE2(ExecutableExpression.class, "execute", Context.class, Object[].class);

        public static final ImmutableMap<Method, BuiltInMethod> MAP;

        static {
            final ImmutableMap.Builder<Method, BuiltInMethod> builder =
                ImmutableMap.builder();
            for (BuiltInMethod value : BuiltInMethod.values()) {
                if (value.method != null) {
                    builder.put(value.method, value);
                }
            }
            MAP = builder.build();
        }

        public final Method method;
        public final Constructor constructor;
        public final Field field;

        StormBuiltInMethod(Method method, Constructor constructor, Field field) {
            this.method = method;
            this.constructor = constructor;
            this.field = field;
        }

        /**
         * Defines a method.
         */
        StormBuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
            this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
        }

        /**
         * Defines a constructor.
         */
        StormBuiltInMethod(Class clazz, Class... argumentTypes) {
            this(null, Types.lookupConstructor(clazz, argumentTypes), null);
        }

        /**
         * Defines a field.
         */
        StormBuiltInMethod(Class clazz, String fieldName, boolean dummy) {
            this(null, null, Types.lookupField(clazz, fieldName));
            assert dummy : "dummy value for method overloading must be true";
        }
    }

}
