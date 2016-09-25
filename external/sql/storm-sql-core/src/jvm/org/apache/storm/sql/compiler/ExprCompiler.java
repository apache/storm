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
package org.apache.storm.sql.compiler;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;
import org.apache.storm.sql.runtime.StormSqlFunctions;

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.*;

/**
 * Compile RexNode on top of the Tuple abstraction.
 */
public class ExprCompiler implements RexVisitor<String> {
  private final PrintWriter pw;
  private final JavaTypeFactory typeFactory;
  private static final ImpTable IMP_TABLE = new ImpTable();
  private int nameCount;

  public ExprCompiler(PrintWriter pw, JavaTypeFactory typeFactory) {
    this.pw = pw;
    this.typeFactory = typeFactory;
  }

  @Override
  public String visitInputRef(RexInputRef rexInputRef) {
    String name = reserveName();
    String typeName = javaTypeName(rexInputRef);
    String boxedTypeName = boxedJavaTypeName(rexInputRef);
    pw.print(String.format("%s %s = (%s)(_data.get(%d));\n", typeName, name,
                           boxedTypeName, rexInputRef.getIndex()));
    return name;
  }

  @Override
  public String visitLocalRef(RexLocalRef rexLocalRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String visitLiteral(RexLiteral rexLiteral) {
    Object v = rexLiteral.getValue();
    RelDataType ty = rexLiteral.getType();
    switch(rexLiteral.getTypeName()) {
      case BOOLEAN:
        return v.toString();
      case CHAR:
        return CompilerUtil.escapeJavaString(((NlsString) v).getValue(), true);
      case NULL:
        return "((" + ((Class<?>)typeFactory.getJavaClass(ty)).getCanonicalName() + ")null)";
      case DOUBLE:
      case BIGINT:
      case DECIMAL:
        switch (ty.getSqlTypeName()) {
          case TINYINT:
          case SMALLINT:
          case INTEGER:
            return Long.toString(((BigDecimal) v).longValueExact());
          case BIGINT:
            return Long.toString(((BigDecimal)v).longValueExact()) + 'L';
          case DECIMAL:
          case FLOAT:
          case REAL:
          case DOUBLE:
            return Util.toScientificNotation((BigDecimal) v);
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return null;
  }

  @Override
  public String visitCall(RexCall rexCall) {
    return IMP_TABLE.compile(this, rexCall);
  }

  @Override
  public String visitOver(RexOver rexOver) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String visitCorrelVariable(
      RexCorrelVariable rexCorrelVariable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String visitDynamicParam(
      RexDynamicParam rexDynamicParam) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String visitRangeRef(RexRangeRef rexRangeRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String visitFieldAccess(
      RexFieldAccess rexFieldAccess) {
    throw new UnsupportedOperationException();
  }

  private String javaTypeName(RexNode node) {
    Type ty = typeFactory.getJavaClass(node.getType());
    return ((Class<?>)ty).getCanonicalName();
  }

  private String boxedJavaTypeName(RexNode node) {
    Type ty = typeFactory.getJavaClass(node.getType());
    Class clazz = (Class<?>)ty;
    if (clazz.isPrimitive()) {
      clazz = Primitives.wrap(clazz);
    }

    return clazz.getCanonicalName();
  }

  private String reserveName() {
    return "t" + ++nameCount;
  }

  // Only generate inline expressions when comparing primitive types
  private boolean primitiveCompareExpr(SqlOperator op, RelDataType type) {
    final Primitive primitive = Primitive.ofBoxOr(typeFactory.getJavaClass(type));
    return primitive != null &&
        (op == LESS_THAN || op == LESS_THAN_OR_EQUAL || op == GREATER_THAN || op == GREATER_THAN_OR_EQUAL);
  }

  private interface CallExprPrinter {
    String translate(ExprCompiler compiler, RexCall call);
  }

  /**
   * Inspired by Calcite's RexImpTable, the ImpTable class maps the operators
   * to their corresponding implementation that generates the expressions in
   * the format of Java source code.
   */
  private static class ImpTable {
    private final Map<SqlOperator, CallExprPrinter> translators;

    private ImpTable() {
      ImmutableMap.Builder<SqlOperator, CallExprPrinter> builder =
          ImmutableMap.builder();
      builder
          .put(builtInMethod(UPPER, BuiltInMethod.UPPER, NullPolicy.STRICT))
          .put(builtInMethod(LOWER, BuiltInMethod.LOWER, NullPolicy.STRICT))
          .put(builtInMethod(INITCAP, BuiltInMethod.INITCAP, NullPolicy.STRICT))
          .put(builtInMethod(SUBSTRING, BuiltInMethod.SUBSTRING, NullPolicy.STRICT))
          .put(builtInMethod(CHARACTER_LENGTH, BuiltInMethod.CHAR_LENGTH, NullPolicy.STRICT))
          .put(builtInMethod(CHAR_LENGTH, BuiltInMethod.CHAR_LENGTH, NullPolicy.STRICT))
          .put(builtInMethod(CONCAT, BuiltInMethod.STRING_CONCAT, NullPolicy.STRICT))
          .put(builtInMethod(ITEM, BuiltInMethod.ANY_ITEM, NullPolicy.STRICT))
          .put(builtInMethod(LIKE, BuiltInMethod.LIKE, NullPolicy.STRICT))
          .put(builtInMethod(SIMILAR_TO, BuiltInMethod.SIMILAR, NullPolicy.STRICT))
          .put(infixBinary(LESS_THAN, "<", "lt"))
          .put(infixBinary(LESS_THAN_OR_EQUAL, "<=", "le"))
          .put(infixBinary(GREATER_THAN, ">", "gt"))
          .put(infixBinary(GREATER_THAN_OR_EQUAL, ">=", "ge"))
          .put(infixBinary(EQUALS, "==", StormSqlFunctions.class, "eq"))
          .put(infixBinary(NOT_EQUALS, "<>", StormSqlFunctions.class, "ne"))
          .put(infixBinary(PLUS, "+", "plus"))
          .put(infixBinary(MINUS, "-", "minus"))
          .put(infixBinary(MULTIPLY, "*", "multiply"))
          .put(infixBinary(DIVIDE, "/", "divide"))
          .put(infixBinary(DIVIDE_INTEGER, "/", "divide"))
          .put(expect(IS_NULL, null))
          .put(expectNot(IS_NOT_NULL, null))
          .put(expect(IS_TRUE, true))
          .put(expectNot(IS_NOT_TRUE, true))
          .put(expect(IS_FALSE, false))
          .put(expectNot(IS_NOT_FALSE, false))
          .put(AND, AND_EXPR)
          .put(OR, OR_EXPR)
          .put(NOT, NOT_EXPR)
          .put(CAST, CAST_EXPR)
          .put(CASE, CASE_EXPR);
      this.translators = builder.build();
    }

    private CallExprPrinter getCallExprPrinter(SqlOperator op) {
      if (translators.containsKey(op)) {
        return translators.get(op);
      } else if (op instanceof SqlUserDefinedFunction) {
        Function function = ((SqlUserDefinedFunction) op).getFunction();
        if (function instanceof ReflectiveFunctionBase) {
          Method method = ((ReflectiveFunctionBase) function).method;
          return methodCall(op, method, NullPolicy.STRICT).getValue();
        }
      }
      return null;
    }

    private String compile(ExprCompiler compiler, RexCall call) {
      SqlOperator op = call.getOperator();
      CallExprPrinter printer = getCallExprPrinter(op);
      if (printer == null) {
        throw new UnsupportedOperationException();
      } else {
        return printer.translate(compiler, call);
      }
    }

    private Map.Entry<SqlOperator, CallExprPrinter> methodCall(
            final SqlOperator op, final Method method, NullPolicy nullPolicy) {
      if (nullPolicy != NullPolicy.STRICT) {
        throw new UnsupportedOperationException();
      }
      CallExprPrinter printer = new CallExprPrinter() {
        @Override
        public String translate(ExprCompiler compiler, RexCall call) {
          PrintWriter pw = compiler.pw;
          String val = compiler.reserveName();
          pw.print(String.format("final %s %s;\n", compiler.javaTypeName(call), val));
          List<String> args = new ArrayList<>();
          for (RexNode op : call.getOperands()) {
            args.add(op.accept(compiler));
          }
          pw.print("if (false) {}\n");
          for (int i = 0; i < args.size(); ++i) {
            String arg = args.get(i);
            if (call.getOperands().get(i).getType().isNullable()) {
              pw.print(String.format("else if (%2$s == null) { %1$s = null; }\n", val, arg));
            }
          }
          String calc = printMethodCall(method, args);
          pw.print(String.format("else { %1$s = %2$s; }\n", val, calc));
          return val;
        }
      };
      return new AbstractMap.SimpleImmutableEntry<>(op, printer);
    }

    private Map.Entry<SqlOperator, CallExprPrinter> builtInMethod(
        final SqlOperator op, final BuiltInMethod method, NullPolicy nullPolicy) {
      return methodCall(op, method.method, nullPolicy);
    }

    private Map.Entry<SqlOperator, CallExprPrinter> infixBinary
        (final SqlOperator op, final String javaOperator, final Class<?> clazz, final String backupMethodName) {
      CallExprPrinter trans = new CallExprPrinter() {
        @Override
        public String translate(
            ExprCompiler compiler, RexCall call) {
          int size = call.getOperands().size();
          assert size == 2;
          String val = compiler.reserveName();
          RexNode op0 = call.getOperands().get(0);
          RexNode op1 = call.getOperands().get(1);
          PrintWriter pw = compiler.pw;
          if (backupMethodName != null) {
            if (!compiler.primitiveCompareExpr(op, op0.getType())) {
              String lhs = op0.accept(compiler);
              String rhs = op1.accept(compiler);
              pw.print(String.format("%s %s = %s;\n", compiler.javaTypeName(call), val,
                  printMethodCall(clazz, backupMethodName, true, Lists.newArrayList(lhs, rhs))));
              return val;
            }
          }
          boolean lhsNullable = op0.getType().isNullable();
          boolean rhsNullable = op1.getType().isNullable();

          pw.print(String.format("final %s %s;\n", compiler.javaTypeName(call), val));
          String lhs = op0.accept(compiler);
          String rhs = op1.accept(compiler);
          pw.print("if (false) {}\n");
          if (lhsNullable) {
            String calc = foldNullExpr(String.format("%s %s %s", lhs, javaOperator, rhs), "null", op1);
            pw.print(String.format("else if (%2$s == null) { %1$s = %3$s; }\n", val, lhs, calc));
          }
          if (rhsNullable) {
            String calc = foldNullExpr(String.format("%s %s %s", lhs, javaOperator, rhs), "null", op0);
            pw.print(String.format("else if (%2$s == null) { %1$s = %3$s; }\n", val, rhs, calc));
          }
          String calc = String.format("%s %s %s", lhs, javaOperator, rhs);
          pw.print(String.format("else { %1$s = %2$s; }\n", val, calc));
          return val;
        }
      };
      return new AbstractMap.SimpleImmutableEntry<>(op, trans);
    }

    private Map.Entry<SqlOperator, CallExprPrinter> infixBinary
        (final SqlOperator op, final String javaOperator, final String backupMethodName) {
      return infixBinary(op, javaOperator, SqlFunctions.class, backupMethodName);
    }

    private Map.Entry<SqlOperator, CallExprPrinter> expect(
        SqlOperator op, final Boolean expect) {
      return expect0(op, expect, false);
    }

    private Map.Entry<SqlOperator, CallExprPrinter> expectNot(
        SqlOperator op, final Boolean expect) {
      return expect0(op, expect, true);
    }

    private Map.Entry<SqlOperator, CallExprPrinter> expect0(
        SqlOperator op, final Boolean expect, final boolean negate) {
      CallExprPrinter trans = new CallExprPrinter() {
        @Override
        public String translate(
            ExprCompiler compiler, RexCall call) {
          assert call.getOperands().size() == 1;
          String val = compiler.reserveName();
          RexNode operand = call.getOperands().get(0);
          boolean nullable = operand.getType().isNullable();
          String op = operand.accept(compiler);
          PrintWriter pw = compiler.pw;
          if (!nullable) {
            if (expect == null) {
              pw.print(String.format("boolean %s = %b;\n", val, !negate));
            } else {
              pw.print(String.format("boolean %s = %s == %b;\n", val, op,
                                     expect ^ negate));
            }
          } else {
            String expr;
            if (expect == null) {
              expr = String.format("%s == null", op);
            } else {
              expr = String.format("%s == Boolean.%s", op, expect ? "TRUE" :
                  "FALSE");
            }
            if (negate) {
              expr = String.format("!(%s)", expr);
            }
            pw.print(String.format("boolean %s = %s;\n", val, expr));
          }
          return val;
        }
      };
      return new AbstractMap.SimpleImmutableEntry<>(op, trans);
    }

    // TODO: AND_EXPR and OR_EXPR have very similar logic, mostly duplicated.
    // If any of the arguments are false, result is false;
    // else if any arguments are null, result is null;
    // else true.
    private static final CallExprPrinter AND_EXPR = new CallExprPrinter() {
      @Override
      public String translate(
          ExprCompiler compiler, RexCall call) {
        String val = compiler.reserveName();
        String valAnyNull = "anyNull_" + compiler.reserveName();

        PrintWriter pw = compiler.pw;
        pw.print(String.format("java.lang.Boolean %s = Boolean.TRUE;\n", val));
        pw.print(String.format("java.lang.Boolean %s = Boolean.FALSE;\n", valAnyNull));

        List<RexNode> operands = call.getOperands();

        for (int idx = 0; idx < operands.size(); idx++) {
          RexNode operand = operands.get(idx);
          pw.print(String.format("// operand #%d\n", idx));
          boolean nullable = operand.getType().isNullable();
          String valEval = operand.accept(compiler);
          String valOp = compiler.reserveName();

          pw.print(String.format("final java.lang.Boolean %s = ", valOp));
          String rhs;
          if (nullable) {
            rhs = foldNullExpr(valEval,
                    String.format("(%1$s != null) ? (%1$s) : ((java.lang.Boolean) null)", valEval),
                    operand);
          } else {
            rhs = valEval;
          }
          pw.print(rhs + ";\n");

          // apply 'nested if' in order to achieve short circuit
          // if val is false, reset the null flag to false to make operation be evaluated to false
          pw.print(String.format("if ((%1$s != null) && !(%1$s)) { %2$s = Boolean.FALSE; %3$s = Boolean.FALSE; }\n", valOp, val, valAnyNull));
          pw.print("else {\n");
          pw.print(String.format("// updating null occurrence with operand #%d\n", idx));
          pw.print(String.format("if ((%1$s == null) && !(%2$s)) { %2$s = Boolean.TRUE; }\n", valOp, valAnyNull));
        }

        for (int i = 0; i < operands.size(); i++) {
          pw.print("}");
        }
        pw.print("\n");

        pw.print(String.format("%1$s = %2$s ? ((java.lang.Boolean) null) : %1$s;\n", val, valAnyNull));

        return val;
      }
    };

    // If any of the arguments are true, result is true;
    // else if any arguments are null, result is null;
    // else false.
    private static final CallExprPrinter OR_EXPR = new CallExprPrinter() {
      @Override
      public String translate(
          ExprCompiler compiler, RexCall call) {
        String val = compiler.reserveName();
        String valAnyNull = "anyNull_" + compiler.reserveName();

        PrintWriter pw = compiler.pw;
        pw.print(String.format("java.lang.Boolean %s = Boolean.FALSE;\n", val));
        pw.print(String.format("java.lang.Boolean %s = Boolean.FALSE;\n", valAnyNull));

        List<RexNode> operands = call.getOperands();

        for (int idx = 0; idx < operands.size(); idx++) {
          RexNode operand = operands.get(idx);
          pw.print(String.format("// operand #%d\n", idx));
          boolean nullable = operand.getType().isNullable();
          String valEval = operand.accept(compiler);
          String valOp = compiler.reserveName();

          pw.print(String.format("final java.lang.Boolean %s = ", valOp));
          String rhs;
          if (nullable) {
            rhs = foldNullExpr(valEval,
                    String.format("(%1$s != null) ? (%1$s) : ((java.lang.Boolean) null)", valEval),
                    operand);
          } else {
            rhs = valEval;
          }
          pw.print(rhs + ";\n");

          // apply 'nested if' in order to achieve short circuit
          // if val is true, reset the null flag to false to make operation be evaluated to true
          pw.print(String.format("if ((%1$s != null) && (%1$s)) { %2$s = Boolean.TRUE; %3$s = Boolean.FALSE; }\n", valOp, val, valAnyNull));
          pw.print("else {\n");
          pw.print(String.format("// updating null occurrence with operand #%d\n", idx));
          pw.print(String.format("if ((%1$s == null) && !(%2$s)) { %2$s = Boolean.TRUE; }\n", valOp, valAnyNull));
        }

        for (int i = 0; i < operands.size(); i++) {
          pw.print("}");
        }
        pw.print("\n");

        pw.print(String.format("%1$s = %2$s ? ((java.lang.Boolean) null) : %1$s;\n", val, valAnyNull));

        return val;
      }
    };

    private static final CallExprPrinter CASE_EXPR = new CallExprPrinter() {
        @Override
        public String translate(
                ExprCompiler compiler, RexCall call) {
            String val = compiler.reserveName();

            PrintWriter pw = compiler.pw;

            pw.print(String.format("%1$s %2$s = null;\n", compiler.javaTypeName(call), val));

            List<RexNode> operands = call.getOperands();

            for (int idx = 0; idx < operands.size() / 2; idx++) {
                RexNode whenOp = operands.get(idx * 2);
                RexNode assignOp = operands.get(idx * 2 + 1);

                pw.print(String.format("// WHEN #%d THEN #%d\n", idx * 2, idx * 2 + 1));

                String valWhen = whenOp.accept(compiler);
                String valAssign = assignOp.accept(compiler);

                // apply 'nested if' in order to achieve short circuit
                pw.print(String.format("if (%1$s == true) { %2$s = %3$s; }\n", valWhen, val, valAssign));
                pw.print("else {\n");
            }

            if (operands.size() % 2 == 1) {
                pw.print("// ELSE\n");
                // the last operand is for 'else'
                String valElseAssign = operands.get(operands.size() - 1).accept(compiler);
                pw.print(String.format("%1$s = %2$s;\n", val, valElseAssign));
            }

            for (int i = 0; i < operands.size() / 2; i++) {
                pw.print("}");
            }
            pw.print("\n");

            return val;
        }
    };

    private static final CallExprPrinter NOT_EXPR = new CallExprPrinter() {
      @Override
      public String translate(
          ExprCompiler compiler, RexCall call) {
        Boolean b = new Boolean(false);
        String val = compiler.reserveName();
        PrintWriter pw = compiler.pw;
        RexNode op = call.getOperands().get(0);
        String lhs = op.accept(compiler);
        boolean nullable = call.getType().isNullable();
        pw.print(String.format("final %s %s;\n", compiler.javaTypeName(call),
                               val));
        if (!nullable) {
          pw.print(String.format("%1$s = !(%2$s);\n", val, lhs));
        } else {
          String s = foldNullExpr(
              String.format("(%1$s == null) ? ((%2$s) null) : !(%1$s)", lhs, compiler.javaTypeName(call)), "null", op);
          pw.print(String.format("%1$s = %2$s;\n", val, s));
        }
        return val;
      }
    };


    private static final CallExprPrinter CAST_EXPR = new CallExprPrinter() {
      @Override
      public String translate(
              ExprCompiler compiler, RexCall call) {
        String val = compiler.reserveName();
        PrintWriter pw = compiler.pw;
        RexNode op = call.getOperands().get(0);
        String lhs = op.accept(compiler);
        pw.print(String.format("final %1$s %2$s = (%1$s) %3$s;\n",
                               compiler.boxedJavaTypeName(call), val, lhs));
        return val;
      }
    };
  }

  private static String foldNullExpr(String notNullExpr, String
      nullExpr, RexNode op) {
    if (op instanceof RexLiteral && ((RexLiteral)op).getTypeName() == SqlTypeName.NULL) {
      return nullExpr;
    } else {
      return notNullExpr;
    }
  }

  public static String printMethodCall(Method method, List<String> args) {
    return printMethodCall(method.getDeclaringClass(), method.getName(),
        Modifier.isStatic(method.getModifiers()), args);
  }

  private static String printMethodCall(Class<?> clazz, String method, boolean isStatic, List<String> args) {
    if (isStatic) {
      return String.format("%s.%s(%s)", clazz.getCanonicalName(), method, Joiner.on(',').join(args));
    } else {
      return String.format("%s.%s(%s)", args.get(0), method,
          Joiner.on(',').join(args.subList(1, args.size())));
    }
  }

}
