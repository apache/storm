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
package org.apache.storm.sql.compiler.backends.standalone;

import com.google.common.base.Joiner;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.stream.Delta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.storm.sql.compiler.RexNodeToJavaCodeCompiler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compile RelNodes into individual functions.
 */
class RelNodeCompiler extends PostOrderRelNodeVisitor<Void> {
  public static Joiner NEW_LINE_JOINER = Joiner.on('\n');

  private final PrintWriter pw;
  private final JavaTypeFactory typeFactory;
  private final RexNodeToJavaCodeCompiler rexCompiler;

  private static final String STAGE_PROLOGUE = NEW_LINE_JOINER.join(
    "  private static final ChannelHandler %1$s = ",
    "    new AbstractChannelHandler() {",
    "    @Override",
    "    public void dataReceived(ChannelContext ctx, Values _data) {",
    ""
  );

  private static final String AGGREGATE_STAGE_PROLOGUE = NEW_LINE_JOINER.join(
          "  private static final ChannelHandler %1$s = ",
          "    new AbstractChannelHandler() {",
          "    private final Values EMPTY_VALUES = new Values();",
          "    private final Map<List<Object>, Map<String, Object>> state = new LinkedHashMap<>();",
          "    private final int[] groupIndices = new int[] {%2$s};",
          "    private List<Object> getGroupValues(Values _data) {",
          "      List<Object> res = new ArrayList<>();",
          "      for (int i: groupIndices) {",
          "        res.add(_data.get(i));",
          "      }",
          "      return res;",
          "    }",
          "",
          "    @Override",
          "    public void flush(ChannelContext ctx) {",
          "      emitAggregateResults(ctx);",
          "      super.flush(ctx);",
          "      state.clear();",
          "    }",
          "",
          "    private void emitAggregateResults(ChannelContext ctx) {",
          "        for (Map.Entry<List<Object>, Map<String, Object>> entry: state.entrySet()) {",
          "          List<Object> groupValues = entry.getKey();",
          "          Map<String, Object> accumulators = entry.getValue();",
          "          %3$s",
          "        }",
          "    }",
          "",
          "    @Override",
          "    public void dataReceived(ChannelContext ctx, Values _data) {",
          ""
  );

  private static final String JOIN_STAGE_PROLOGUE = NEW_LINE_JOINER.join(
          "  private static final ChannelHandler %1$s = ",
          "    new AbstractChannelHandler() {",
          "      Object left = %2$s;",
          "      Object right = %3$s;",
          "      Object source = null;",
          "      List<Values> leftRows = new ArrayList<>();",
          "      List<Values> rightRows = new ArrayList<>();",
          "      boolean leftDone = false;",
          "      boolean rightDone = false;",
          "      int[] ordinals = new int[] {%4$s, %5$s};",
          "",
          "      Multimap<Object, Values> getJoinTable(List<Values> rows, int joinIndex) {",
          "         Multimap<Object, Values> m = ArrayListMultimap.create();",
          "         for(Values v: rows) {",
          "           m.put(v.get(joinIndex), v);",
          "         }",
          "         return m;",
          "      }",
          "",
          "      List<Values> join(Multimap<Object, Values> tab, List<Values> rows, int rowIdx, boolean rev) {",
          "         List<Values> res = new ArrayList<>();",
          "         for (Values row: rows) {",
          "           for (Values mapValue: tab.get(row.get(rowIdx))) {",
          "             if (mapValue != null) {",
          "               Values joinedRow = new Values();",
          "               if(rev) {",
          "                 joinedRow.addAll(row);",
          "                 joinedRow.addAll(mapValue);",
          "               } else {",
          "                 joinedRow.addAll(mapValue);",
          "                 joinedRow.addAll(row);",
          "               }",
          "               res.add(joinedRow);",
          "             }",
          "           }",
          "         }",
          "         return res;",
          "      }",
          "",
          "    @Override",
          "    public void setSource(ChannelContext ctx, Object source) {",
          "      this.source = source;",
          "    }",
          "",
          "    @Override",
          "    public void flush(ChannelContext ctx) {",
          "        if (source == left) {",
          "            leftDone = true;",
          "        } else if (source == right) {",
          "            rightDone = true;",
          "        }",
          "        if (leftDone && rightDone) {",
          "          if (leftRows.size() <= rightRows.size()) {",
          "            for(Values res: join(getJoinTable(leftRows, ordinals[0]), rightRows, ordinals[1], false)) {",
          "              ctx.emit(res);",
          "            }",
          "          } else {",
          "            for(Values res: join(getJoinTable(rightRows, ordinals[1]), leftRows, ordinals[0], true)) {",
          "              ctx.emit(res);",
          "            }",
          "          }",
          "          leftDone = rightDone = false;",
          "          leftRows.clear();",
          "          rightRows.clear();",
          "          super.flush(ctx);",
          "        }",
          "    }",
          "",
          "    @Override",
          "    public void dataReceived(ChannelContext ctx, Values _data) {",
          ""
  );

  private static final String STAGE_PASSTHROUGH = NEW_LINE_JOINER.join(
      "  private static final ChannelHandler %1$s = AbstractChannelHandler.PASS_THROUGH;",
      "");

  private static final String STAGE_ENUMERABLE_TABLE_SCAN = NEW_LINE_JOINER.join(
          "  private static final ChannelHandler %1$s = new AbstractChannelHandler() {",
          "    @Override",
          "    public void flush(ChannelContext ctx) {",
          "      ctx.setSource(this);",
          "      super.flush(ctx);",
          "    }",
          "",
          "    @Override",
          "    public void dataReceived(ChannelContext ctx, Values _data) {",
          "      ctx.setSource(this);",
          "      ctx.emit(_data);",
          "    }",
          "  };",
          "");

  private int nameCount;
  private Map<AggregateCall, String> aggregateCallVarNames = new HashMap<>();

  RelNodeCompiler(PrintWriter pw, JavaTypeFactory typeFactory) {
    this.pw = pw;
    this.typeFactory = typeFactory;
    this.rexCompiler = new RexNodeToJavaCodeCompiler(new RexBuilder(typeFactory));
  }

  @Override
  public Void visitDelta(Delta delta, List<Void> inputStreams) throws Exception {
    pw.print(String.format(STAGE_PASSTHROUGH, getStageName(delta)));
    return null;
  }

  @Override
  public Void visitFilter(Filter filter, List<Void> inputStreams) throws Exception {
    beginStage(filter);

    List<RexNode> childExps = filter.getChildExps();
    RelDataType inputRowType = filter.getInput(0).getRowType();

    pw.print("Context context = new StormContext(Processor.dataContext);\n");
    pw.print("context.values = _data.toArray();\n");
    pw.print("Object[] outputValues = new Object[1];\n");

    pw.write(rexCompiler.compileToBlock(childExps, inputRowType).toString());

    String r = "((Boolean) outputValues[0])";
    if (filter.getCondition().getType().isNullable()) {
      pw.print(String.format("    if (%s != null && %s) { ctx.emit(_data); }\n", r, r));
    } else {
      pw.print(String.format("    if (%s) { ctx.emit(_data); }\n", r, r));
    }
    endStage();
    return null;
  }

  @Override
  public Void visitProject(Project project, List<Void> inputStreams) throws Exception {
    beginStage(project);

    List<RexNode> childExps = project.getChildExps();
    RelDataType inputRowType = project.getInput(0).getRowType();
    int outputCount = project.getRowType().getFieldCount();

    pw.print("Context context = new StormContext(Processor.dataContext);\n");
    pw.print("context.values = _data.toArray();\n");
    pw.print(String.format("Object[] outputValues = new Object[%d];\n", outputCount));

    pw.write(rexCompiler.compileToBlock(childExps, inputRowType).toString());

    pw.print("    ctx.emit(new Values(outputValues));\n");
    endStage();
    return null;
  }

  @Override
  public Void defaultValue(RelNode n, List<Void> inputStreams) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visitTableScan(TableScan scan, List<Void> inputStreams) throws Exception {
    pw.print(String.format(STAGE_ENUMERABLE_TABLE_SCAN, getStageName(scan)));
    return null;
  }

  @Override
  public Void visitAggregate(Aggregate aggregate, List<Void> inputStreams) throws Exception {
    beginAggregateStage(aggregate);
    pw.println("        if (_data != null) {");
    pw.println("        List<Object> curGroupValues = getGroupValues(_data);");
    pw.println("        if (!state.containsKey(curGroupValues)) {");
    pw.println("          state.put(curGroupValues, new HashMap<String, Object>());");
    pw.println("        }");
    pw.println("        Map<String, Object> accumulators = state.get(curGroupValues);");
    for (AggregateCall call : aggregate.getAggCallList()) {
      aggregate(call);
    }
    pw.println("        }");
    endStage();
    return null;
  }

  @Override
  public Void visitJoin(Join join, List<Void> inputStreams) {
    beginJoinStage(join);
    pw.println("        if (source == left) {");
    pw.println("            leftRows.add(_data);");
    pw.println("        } else if (source == right) {");
    pw.println("            rightRows.add(_data);");
    pw.println("        }");
    endStage();
    return null;
  }

  private String groupValueEmitStr(String var, int n) {
    int count = 0;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) {
      if (++count > 1) {
        sb.append(", ");
      }
      sb.append(var).append(".").append("get(").append(i).append(")");
    }
    return sb.toString();
  }

  private String emitAggregateStmts(Aggregate aggregate) {
    List<String> res = new ArrayList<>();
    StringWriter sw = new StringWriter();
    for (AggregateCall call : aggregate.getAggCallList()) {
      res.add(aggregateResult(call, new PrintWriter(sw)));
    }
    return NEW_LINE_JOINER.join(sw.toString(),
                                String.format("          ctx.emit(new Values(%s, %s));",
                                              groupValueEmitStr("groupValues", aggregate.getGroupSet().cardinality()),
                                              Joiner.on(", ").join(res)));
  }

  private String aggregateResult(AggregateCall call, PrintWriter pw) {
    SqlAggFunction aggFunction = call.getAggregation();
    String aggregationName = call.getAggregation().getName();
    Type ty = typeFactory.getJavaClass(call.getType());
    String result;
    if (aggFunction instanceof SqlUserDefinedAggFunction) {
      AggregateFunction aggregateFunction = ((SqlUserDefinedAggFunction) aggFunction).function;
      result = doAggregateResult((AggregateFunctionImpl) aggregateFunction, reserveAggVarName(call), ty, pw);
    } else {
      List<BuiltinAggregateFunctions.TypeClass> typeClasses = BuiltinAggregateFunctions.TABLE.get(aggregationName);
      if (typeClasses == null) {
        throw new UnsupportedOperationException(aggregationName + " Not implemented");
      }
      result = doAggregateResult(AggregateFunctionImpl.create(findMatchingClass(aggregationName, typeClasses, ty)),
                                 reserveAggVarName(call), ty, pw);
    }
    return result;
  }

  private String doAggregateResult(AggregateFunctionImpl aggFn, String varName, Type ty, PrintWriter pw) {
    String resultName = varName + "_result";
    Class<?> accumulatorType = aggFn.accumulatorType;
    Class<?> resultType = aggFn.resultType;
    List<String> args = new ArrayList<>();
    if (!aggFn.isStatic) {
      String aggObjName = String.format("%s_obj", varName);
      String aggObjClassName = aggFn.initMethod.getDeclaringClass().getCanonicalName();
      pw.println("          @SuppressWarnings(\"unchecked\")");
      pw.println(String.format("          final %1$s %2$s = (%1$s) accumulators.get(\"%2$s\");", aggObjClassName,
              aggObjName));
      args.add(aggObjName);
    }
    args.add(String.format("(%s)accumulators.get(\"%s\")", accumulatorType.getCanonicalName(), varName));
    pw.println(String.format("          final %s %s = %s;", resultType.getCanonicalName(),
                             resultName, printMethodCall(aggFn.resultMethod, args)));

    return resultName;
  }

  private void aggregate(AggregateCall call) {
    SqlAggFunction aggFunction = call.getAggregation();
    String aggregationName = call.getAggregation().getName();
    Type ty = typeFactory.getJavaClass(call.getType());
    if (call.getArgList().size() != 1) {
      if (aggregationName.equals("COUNT")) {
        if (call.getArgList().size() != 0) {
          throw new UnsupportedOperationException("Count with nullable fields");
        }
      }
    }
    if (aggFunction instanceof SqlUserDefinedAggFunction) {
      AggregateFunction aggregateFunction = ((SqlUserDefinedAggFunction) aggFunction).function;
      doAggregate((AggregateFunctionImpl) aggregateFunction, reserveAggVarName(call), ty, call.getArgList());
    } else {
      List<BuiltinAggregateFunctions.TypeClass> typeClasses = BuiltinAggregateFunctions.TABLE.get(aggregationName);
      if (typeClasses == null) {
        throw new UnsupportedOperationException(aggregationName + " Not implemented");
      }
      doAggregate(AggregateFunctionImpl.create(findMatchingClass(aggregationName, typeClasses, ty)),
                  reserveAggVarName(call), ty, call.getArgList());
    }
  }

  private Class<?> findMatchingClass(String aggregationName, List<BuiltinAggregateFunctions.TypeClass> typeClasses, Type ty) {
    for (BuiltinAggregateFunctions.TypeClass typeClass : typeClasses) {
      if (typeClass.ty.equals(BuiltinAggregateFunctions.TypeClass.GenericType.class) || typeClass.ty.equals(ty)) {
        return typeClass.clazz;
      }
    }
    throw new UnsupportedOperationException(aggregationName + " Not implemeted for type '" + ty + "'");
  }

  private void doAggregate(AggregateFunctionImpl aggFn, String varName, Type ty, List<Integer> argList) {
    List<String> args = new ArrayList<>();
    Class<?> accumulatorType = aggFn.accumulatorType;
    if (!aggFn.isStatic) {
      String aggObjName = String.format("%s_obj", varName);
      String aggObjClassName = aggFn.initMethod.getDeclaringClass().getCanonicalName();
      pw.println(String.format("          if (!accumulators.containsKey(\"%s\")) { ", aggObjName));
      pw.println(String.format("            accumulators.put(\"%s\", new %s());", aggObjName, aggObjClassName));
      pw.println("          }");
      pw.println("          @SuppressWarnings(\"unchecked\")");
      pw.println(String.format("          final %1$s %2$s = (%1$s) accumulators.get(\"%2$s\");", aggObjClassName,
              aggObjName));
      args.add(aggObjName);
    }
    args.add(String.format("%1$s == null ? %2$s : (%3$s) %1$s",
                           "accumulators.get(\"" + varName + "\")",
                           printMethodCall(aggFn.initMethod, args),
                           accumulatorType.getCanonicalName()));
    if (argList.isEmpty()) {
      args.add("EMPTY_VALUES");
    } else {
      for (int i = 0; i < aggFn.valueTypes.size(); i++) {
        args.add(String.format("(%s) %s", aggFn.valueTypes.get(i).getCanonicalName(), "_data.get(" + argList.get(i) + ")"));
      }
    }
    pw.print(String.format("          accumulators.put(\"%s\", %s);\n",
                           varName,
                           printMethodCall(aggFn.addMethod, args)));
  }

  private String reserveAggVarName(AggregateCall call) {
    String varName;
    if ((varName = aggregateCallVarNames.get(call)) == null) {
      varName = call.getAggregation().getName() + ++nameCount;
      aggregateCallVarNames.put(call, varName);
    }
    return varName;
  }

  private void beginStage(RelNode n) {
    pw.print(String.format(STAGE_PROLOGUE, getStageName(n)));
  }

  private void beginAggregateStage(Aggregate n) {
    pw.print(String.format(AGGREGATE_STAGE_PROLOGUE, getStageName(n), getGroupByIndices(n), emitAggregateStmts(n)));
  }

  private void beginJoinStage(Join join) {
    int[] ordinals = new int[2];
    if (!RelOptUtil.analyzeSimpleEquiJoin((LogicalJoin) join, ordinals)) {
      throw new UnsupportedOperationException("Only simple equi joins are supported");
    }

    pw.print(String.format(JOIN_STAGE_PROLOGUE, getStageName(join),
                           getStageName(join.getLeft()),
                           getStageName(join.getRight()),
                           ordinals[0],
                           ordinals[1]));
  }

  private void endStage() {
    pw.print("  }\n  };\n");
  }

  static String getStageName(RelNode n) {
    return n.getClass().getSimpleName().toUpperCase() + "_" + n.getId();
  }

  private String getGroupByIndices(Aggregate n) {
    StringBuilder res = new StringBuilder();
    int count = 0;
    for (int i : n.getGroupSet()) {
      if (++count > 1) {
        res.append(", ");
      }
      res.append(i);
    }
    return res.toString();
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
