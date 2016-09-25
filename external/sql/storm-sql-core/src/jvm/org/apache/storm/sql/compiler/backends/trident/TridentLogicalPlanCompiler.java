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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.sql.compiler.backends.trident;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Primitives;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.storm.sql.compiler.ExprCompiler;
import org.apache.storm.sql.compiler.PostOrderRelNodeVisitor;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.trident.functions.EvaluationFilter;
import org.apache.storm.sql.runtime.trident.functions.EvaluationFunction;
import org.apache.storm.sql.runtime.trident.functions.ForwardFunction;
import org.apache.storm.sql.runtime.trident.functions.UDAFWrappedAggregator;
import org.apache.storm.sql.runtime.trident.operations.CountBy;
import org.apache.storm.sql.runtime.trident.operations.DivideForAverage;
import org.apache.storm.sql.runtime.trident.operations.MaxBy;
import org.apache.storm.sql.runtime.trident.operations.MinBy;
import org.apache.storm.sql.runtime.trident.operations.SumBy;
import org.apache.storm.trident.JoinOutFieldsMode;
import org.apache.storm.trident.JoinType;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.ChainedAggregatorDeclarer;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.fluent.IAggregatableStream;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class TransformInformation {
    private Fields inputFields;
    private Function function;
    private Fields functionFields;

    public TransformInformation(Fields inputFields, Function function, Fields functionFields) {
        this.inputFields = inputFields;
        this.function = function;
        this.functionFields = functionFields;
    }

    public Fields getInputFields() {
        return inputFields;
    }

    public Function getFunction() {
        return function;
    }

    public Fields getFunctionFields() {
        return functionFields;
    }
}

public class TridentLogicalPlanCompiler extends PostOrderRelNodeVisitor<IAggregatableStream> {
    protected final Map<String, ISqlTridentDataSource> sources;
    protected final JavaTypeFactory typeFactory;
    protected TridentTopology topology;

    public TridentLogicalPlanCompiler(Map<String, ISqlTridentDataSource> sources, JavaTypeFactory typeFactory, TridentTopology topology) {
        this.sources = sources;
        this.typeFactory = typeFactory;
        this.topology = topology;
    }

    @Override
    public IAggregatableStream defaultValue(RelNode n, List<IAggregatableStream> inputStreams) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IAggregatableStream visitTableScan(TableScan scan, List<IAggregatableStream> inputStreams) throws Exception {
        String sourceName = Joiner.on('.').join(scan.getTable().getQualifiedName());
        if (!sources.containsKey(sourceName)) {
            throw new RuntimeException("Cannot find table " + sourceName);
        }

        String stageName = getStageName(scan);
        return topology.newStream(stageName, sources.get(sourceName).getProducer());
    }

    @Override
    public IAggregatableStream visitTableModify(TableModify modify, List<IAggregatableStream> inputStreams) throws Exception {
        if (inputStreams.size() > 1) {
            throw new RuntimeException("TableModify is a SingleRel");
        }

        Preconditions.checkArgument(modify.isInsert(), "Only INSERT statement is supported.");
        RelNode input = modify.getInput();
        String tableName = Joiner.on('.').join(modify.getTable().getQualifiedName());
        Stream inputStream = inputStreams.get(0).toStream();
        String stageName = getStageName(modify);

        List<String> inputFields = input.getRowType().getFieldNames();
        List<String> outputFields = modify.getRowType().getFieldNames();

        ISqlTridentDataSource.SqlTridentConsumer consumer = sources.get(tableName).getConsumer();

        // In fact this is normally the end of stream, but to match the return type we open new streams based on State values
        return inputStream
                .partitionPersist(consumer.getStateFactory(), new Fields(inputFields), consumer.getStateUpdater(),
                        new Fields(outputFields))
                .newValuesStream().name(stageName);
    }

    @Override
    public IAggregatableStream visitJoin(Join join, List<IAggregatableStream> inputStreams) throws Exception {
        if (inputStreams.size() != 2) {
            throw new RuntimeException("Join is a BiRel");
        }

        int[] ordinals = new int[2];
        if (!RelOptUtil.analyzeSimpleEquiJoin((LogicalJoin) join, ordinals)) {
            throw new UnsupportedOperationException("Only simple equi joins are supported");
        }

        List<JoinType> joinTypes = new ArrayList<>();
        switch (join.getJoinType()) {
            case INNER:
                joinTypes.add(JoinType.INNER);
                joinTypes.add(JoinType.INNER);
                break;

            case LEFT:
                joinTypes.add(JoinType.INNER);
                joinTypes.add(JoinType.OUTER);
                break;

            case RIGHT:
                joinTypes.add(JoinType.OUTER);
                joinTypes.add(JoinType.INNER);
                break;

            case FULL:
                joinTypes.add(JoinType.OUTER);
                joinTypes.add(JoinType.OUTER);
                break;

            default:
                throw new UnsupportedOperationException("Unsupported join type: " + join.getJoinType());
        }

        String leftJoinFieldName = join.getLeft().getRowType().getFieldNames().get(ordinals[0]);
        String rightJoinFieldName = join.getRight().getRowType().getFieldNames().get(ordinals[1]);

        Stream leftInputStream = inputStreams.get(0).toStream();
        Stream rightInputStream = inputStreams.get(1).toStream();
        String stageName = getStageName(join);

        return topology
                .join(leftInputStream, new Fields(leftJoinFieldName), rightInputStream, new Fields(rightJoinFieldName),
                        new Fields(join.getRowType().getFieldNames()), joinTypes, JoinOutFieldsMode.PRESERVE)
                .name(stageName);
    }

    @Override
    public IAggregatableStream visitProject(Project project, List<IAggregatableStream> inputStreams) throws Exception {
        if (inputStreams.size() != 1) {
            throw new RuntimeException("Project is a SingleRel");
        }

        Stream inputStream = inputStreams.get(0).toStream();
        Fields inputFields = inputStream.getOutputFields();
        String stageName = getStageName(project);

        // Trident doesn't allow duplicated field name... need to do the trick...
        List<String> outputFieldNames = project.getRowType().getFieldNames();
        List<String> temporaryOutputFieldNames = new ArrayList<>();
        for (String outputFieldName : outputFieldNames) {
            temporaryOutputFieldNames.add("__" + outputFieldName + "__");
        }

        try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw)) {
            pw.write("import org.apache.storm.tuple.Values;\n");

            ExprCompiler compiler = new ExprCompiler(pw, typeFactory);

            int size = project.getChildExps().size();
            String[] res = new String[size];
            for (int i = 0; i < size; ++i) {
                res[i] = project.getChildExps().get(i).accept(compiler);
            }

            pw.write(String.format("\nreturn new Values(%s);", Joiner.on(',').join(res)));
            final String expression = sw.toString();

            return inputStream.each(inputFields, new EvaluationFunction(expression), new Fields(temporaryOutputFieldNames))
                    .project(new Fields(temporaryOutputFieldNames))
                    .each(new Fields(temporaryOutputFieldNames), new ForwardFunction(), new Fields(outputFieldNames))
                    .project(new Fields(outputFieldNames))
                    .name(stageName);
        }
    }

    @Override
    public IAggregatableStream visitFilter(Filter filter, List<IAggregatableStream> inputStreams) throws Exception {
        if (inputStreams.size() != 1) {
            throw new RuntimeException("Filter is a SingleRel");
        }

        Stream inputStream = inputStreams.get(0).toStream();
        String stageName = getStageName(filter);

        try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw)) {
            ExprCompiler compiler = new ExprCompiler(pw, typeFactory);
            String ret = filter.getCondition().accept(compiler);
            pw.write(String.format("\nreturn %s;", ret));
            final String expression = sw.toString();

            return inputStream.filter(new EvaluationFilter(expression)).name(stageName);
        }
    }

    @Override
    public IAggregatableStream visitAggregate(Aggregate aggregate, List<IAggregatableStream> inputStreams) throws Exception {
        if (inputStreams.size() != 1) {
            throw new RuntimeException("Aggregate is a SingleRel");
        }

        Stream inputStream = inputStreams.get(0).toStream();
        String stageName = getStageName(aggregate);

        List<String> groupByFieldNames = new ArrayList<>();
        for (Integer idx : aggregate.getGroupSet()) {
            String fieldName = inputStream.getOutputFields().get(idx);
            groupByFieldNames.add(fieldName);
        }

        Fields groupByFields = new Fields(groupByFieldNames);
        GroupedStream groupedStream = inputStream.groupBy(groupByFields);

        ChainedAggregatorDeclarer chainedAggregatorDeclarer = groupedStream.chainedAgg();
        List<TransformInformation> transformsAfterChained = new ArrayList<>();
        for (AggregateCall call : aggregate.getAggCallList()) {
            appendAggregationInChain(chainedAggregatorDeclarer, groupByFieldNames, inputStream, call, transformsAfterChained);
        }

        Stream stream = chainedAggregatorDeclarer.chainEnd();
        for (TransformInformation transformInformation : transformsAfterChained) {
            stream = stream.each(transformInformation.getInputFields(), transformInformation.getFunction(), transformInformation.getFunctionFields());
        }

        // We're OK to project by Calcite information since each aggregation function create output fields via that information
        List<String> outputFields = aggregate.getRowType().getFieldNames();
        return stream.project(new Fields(outputFields)).name(stageName);
    }

    private void appendAggregationInChain(ChainedAggregatorDeclarer chainedDeclarer, List<String> groupByFieldNames,
                                          Stream inputStream, AggregateCall call, List<TransformInformation> transformsAfterChained) {
        String outputField = call.getName();
        SqlAggFunction aggFunction = call.getAggregation();
        String aggregationName = call.getAggregation().getName();

        // only count can have no argument
        if (call.getArgList().size() != 1) {
            if (aggregationName.toUpperCase().equals("COUNT")) {
                if (call.getArgList().size() > 0) {
                    throw new IllegalArgumentException("COUNT should have one or no(a.k.a '*') argument");
                }
            } else {
                throw new IllegalArgumentException("Aggregate call should have one argument");
            }
        }

        if (aggFunction instanceof SqlUserDefinedAggFunction) {
            appendUDAFToChain(chainedDeclarer, groupByFieldNames, inputStream, call, outputField);
        } else {
            switch (aggregationName.toUpperCase()) {
                case "COUNT":
                    appendCountFunctionToChain(chainedDeclarer, groupByFieldNames, inputStream, call, outputField);
                    break;

                case "MAX":
                    appendMaxFunctionToChain(chainedDeclarer, groupByFieldNames, inputStream, call, outputField);
                    break;

                case "MIN":
                    appendMinFunctionToChain(chainedDeclarer, groupByFieldNames, inputStream, call, outputField);
                    break;

                case "SUM":
                    appendSumFunctionToChain(chainedDeclarer, groupByFieldNames, inputStream, call, outputField);
                    break;

                case "AVG":
                    appendAvgFunctionToChain(chainedDeclarer, groupByFieldNames, inputStream, call, outputField, transformsAfterChained);
                    break;

                default:
                    throw new UnsupportedOperationException("Not supported function: " + aggregationName.toUpperCase());
            }
        }
    }

    private void appendUDAFToChain(ChainedAggregatorDeclarer chainedDeclarer, List<String> groupByFieldNames, Stream inputStream, 
                                   AggregateCall call, String outputField) {
        AggregateFunctionImpl aggFunction = (AggregateFunctionImpl) ((SqlUserDefinedAggFunction) call.getAggregation()).function;

        List<String> inputFields = new ArrayList<>(groupByFieldNames);
        Integer fieldIdx = call.getArgList().get(0);
        String inputFieldName = inputStream.getOutputFields().get(fieldIdx);
        inputFields.add(inputFieldName);

        Class<?> capturedTargetClazz = getAggrCallReturnJavaType(call);

        List<String> outputFields = new ArrayList<>(groupByFieldNames);
        outputFields.add(outputField);

        UDAFWrappedAggregator aggregator = new UDAFWrappedAggregator(aggFunction.declaringClass.getName(),
                aggFunction.isStatic, inputFieldName, capturedTargetClazz);

        chainedDeclarer.aggregate(new Fields(inputFields), aggregator, new Fields(outputField));
    }

    private void appendAvgFunctionToChain(ChainedAggregatorDeclarer chainedDeclarer, List<String> groupByFieldNames,
                                          Stream inputStream, AggregateCall call, String outputField,
                                          List<TransformInformation> transformsAfterChained) {
        List<String> inputFields = new ArrayList<>(groupByFieldNames);
        Integer fieldIdx = call.getArgList().get(0);
        String inputFieldName = inputStream.getOutputFields().get(fieldIdx);
        inputFields.add(inputFieldName);

        Class<?> capturedTargetClazz = getAggrCallReturnJavaType(call);
        if (!Number.class.isAssignableFrom(capturedTargetClazz)) {
            throw new IllegalStateException("Return type of aggregation call should be a Number");
        }

        Class<? extends Number> targetClazz = (Class<? extends Number>) capturedTargetClazz;

        String tempCountFieldName = "__cnt__" + outputField;
        String tempSumFieldName = "__sum__" + outputField;

        chainedDeclarer
                .aggregate(new Fields(inputFields), new CountBy(inputFieldName), new Fields(tempCountFieldName))
                .aggregate(new Fields(inputFields), new SumBy(inputFieldName, targetClazz), new Fields(tempSumFieldName));

        TransformInformation divForAverage = new TransformInformation(
                new Fields(tempSumFieldName, tempCountFieldName),
                new DivideForAverage(targetClazz),
                new Fields(outputField));
        transformsAfterChained.add(divForAverage);
    }

    private void appendCountFunctionToChain(ChainedAggregatorDeclarer chainedDeclarer, List<String> groupByFieldNames,
                                            Stream inputStream, AggregateCall call, String outputField) {
        List<String> inputFields = new ArrayList<>(groupByFieldNames);
        if (call.getArgList().size() == 0) {
            chainedDeclarer.aggregate(new Fields(inputFields), new Count(), new Fields(outputField));
        } else {
            // call.getArgList().size() == 1
            Integer fieldIdx = call.getArgList().get(0);
            String inputFieldName = inputStream.getOutputFields().get(fieldIdx);
            inputFields.add(inputFieldName);
            chainedDeclarer.aggregate(new Fields(inputFields), new CountBy(inputFieldName), new Fields(outputField));
        }
    }

    private void appendMaxFunctionToChain(ChainedAggregatorDeclarer chainedDeclarer, List<String> groupByFieldNames,
                                          Stream inputStream, AggregateCall call, String outputField) {
        List<String> inputFields = new ArrayList<>(groupByFieldNames);
        Integer fieldIdx = call.getArgList().get(0);
        String inputFieldName = inputStream.getOutputFields().get(fieldIdx);
        inputFields.add(inputFieldName);

        Class<?> capturedTargetClazz = getAggrCallReturnJavaType(call);

        chainedDeclarer.aggregate(new Fields(inputFields), new MaxBy(inputFieldName, capturedTargetClazz), new Fields(outputField));
    }

    private void appendMinFunctionToChain(ChainedAggregatorDeclarer chainedDeclarer, List<String> groupByFieldNames, Stream inputStream, AggregateCall call, String outputField) {
        List<String> inputFields = new ArrayList<>(groupByFieldNames);
        Integer fieldIdx = call.getArgList().get(0);
        String inputFieldName = inputStream.getOutputFields().get(fieldIdx);
        inputFields.add(inputFieldName);

        Class<?> capturedTargetClazz = getAggrCallReturnJavaType(call);

        chainedDeclarer.aggregate(new Fields(inputFields), new MinBy(inputFieldName, capturedTargetClazz), new Fields(outputField));
    }

    private void appendSumFunctionToChain(ChainedAggregatorDeclarer chainedDeclarer, List<String> groupByFieldNames, Stream inputStream, AggregateCall call, String outputField) {
        List<String> inputFields = new ArrayList<>(groupByFieldNames);
        Integer fieldIdx = call.getArgList().get(0);
        String inputFieldName = inputStream.getOutputFields().get(fieldIdx);
        inputFields.add(inputFieldName);

        Class<?> capturedTargetClazz = getAggrCallReturnJavaType(call);
        if (!Number.class.isAssignableFrom(capturedTargetClazz)) {
            throw new IllegalStateException("Return type of aggregation call should be a Number");
        }

        Class<? extends Number> targetClazz = (Class<? extends Number>) capturedTargetClazz;

        List<String> outputFields = new ArrayList<>(groupByFieldNames);
        outputFields.add(outputField);
        chainedDeclarer.aggregate(new Fields(inputFields), new SumBy(inputFieldName, targetClazz), new Fields(outputField));
    }

    private String getStageName(RelNode n) {
        return n.getClass().getSimpleName().toUpperCase() + "_" + n.getId();
    }

    private Class<?> getAggrCallReturnJavaType(AggregateCall call) {
        return toClassWithBoxing(typeFactory.getJavaClass(call.getType()));
    }

    private Class toClassWithBoxing(Type type) {
        Class clazz = (Class<?>)type;
        if (clazz.isPrimitive()) {
            clazz = Primitives.wrap(clazz);
        }

        return clazz;
    }
}