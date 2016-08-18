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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.storm.sql.compiler.ExprCompiler;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.trident.functions.EvaluationFilter;
import org.apache.storm.sql.runtime.trident.functions.EvaluationFunction;
import org.apache.storm.sql.runtime.trident.functions.ForwardFunction;
import org.apache.storm.sql.runtime.trident.operations.CountBy;
import org.apache.storm.sql.runtime.trident.operations.DivideAsDouble;
import org.apache.storm.sql.runtime.trident.operations.MaxBy;
import org.apache.storm.sql.runtime.trident.operations.MinBy;
import org.apache.storm.sql.runtime.trident.operations.SumBy;
import org.apache.storm.trident.JoinType;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.fluent.IAggregatableStream;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TridentLogicalPlanCompiler extends TridentPostOrderRelNodeVisitor {
    protected final Map<String, ISqlTridentDataSource> sources;
    protected final JavaTypeFactory typeFactory;

    public TridentLogicalPlanCompiler(Map<String, ISqlTridentDataSource> sources, JavaTypeFactory typeFactory, TridentTopology topology) {
        super(topology);
        this.sources = sources;
        this.typeFactory = typeFactory;
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

        return inputStream.each(new Fields(inputFields), sources.get(tableName).getConsumer(), new Fields(outputFields))
                .name(stageName);
    }

    @Override
    public IAggregatableStream visitProject(Project project, List<IAggregatableStream> inputStreams) throws Exception {
        if (inputStreams.size() > 1) {
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
        if (inputStreams.size() > 1) {
            throw new RuntimeException("Filter is a SingleRel");
        }

        Stream inputStream = inputStreams.get(0).toStream();
        String stageName = getStageName(filter);

        try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw)) {
            pw.write("import org.apache.storm.tuple.Values;\n");

            ExprCompiler compiler = new ExprCompiler(pw, typeFactory);
            String ret = filter.getCondition().accept(compiler);
            pw.write(String.format("\nreturn %s;", ret));
            final String expression = sw.toString();

            return inputStream.filter(new EvaluationFilter(expression)).name(stageName);
        }
    }

    @Override
    public IAggregatableStream visitAggregate(Aggregate aggregate, List<IAggregatableStream> inputStreams) throws Exception {
        if (inputStreams.size() > 1) {
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

        List<Stream> joiningStreams = new ArrayList<>();
        List<Fields> joiningFields = new ArrayList<>();
        List<String> newOutputFields = new ArrayList<>();
        newOutputFields.addAll(groupByFieldNames);

        for (AggregateCall call : aggregate.getAggCallList()) {
            Stream aggregateStream = handleAggregateCall(groupedStream, groupByFieldNames, inputStream, call);

            Fields outputFields = aggregateStream.getOutputFields();
            // assuming handleAggregateCall does project so that new output field is always placed to end of fields
            String newFieldName = outputFields.get(outputFields.size() - 1);

            joiningStreams.add(aggregateStream);
            joiningFields.add(groupByFields);
            newOutputFields.add(newFieldName);
        }

        return topology.join(joiningStreams, joiningFields, new Fields(newOutputFields), JoinType.INNER).name(stageName);
    }

    private Stream handleAggregateCall(GroupedStream groupedStream, List<String> groupByFieldNames, Stream inputStream, AggregateCall call) {
        String outputField = call.getName();
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

        // FIXME: support UDF function
        // FIXME: for now it only supports basic aggregate functions
        switch (aggregationName.toUpperCase()) {
            case "COUNT":
                return handleCountFunction(groupedStream, groupByFieldNames, inputStream, call, outputField);

            case "MAX":
                return handleMaxFunction(groupedStream, groupByFieldNames, inputStream, call, outputField);

            case "MIN":
                return handleMinFunction(groupedStream, groupByFieldNames, inputStream, call, outputField);

            case "SUM":
                return handleSumFunction(groupedStream, groupByFieldNames, inputStream, call, outputField);

            case "AVG":
                return handleAvgFunction(groupedStream, groupByFieldNames, inputStream, call, outputField);

            default:
                throw new UnsupportedOperationException("Not supported function: " + aggregationName.toUpperCase());
        }
    }

    private Stream handleAvgFunction(GroupedStream groupedStream, List<String> groupByFieldNames, Stream inputStream, AggregateCall call, String outputField) {
        List<String> inputFields = new ArrayList<>(groupByFieldNames);
        Integer fieldIdx = call.getArgList().get(0);
        String inputFieldName = inputStream.getOutputFields().get(fieldIdx);
        inputFields.add(inputFieldName);

        List<String> outputFields = new ArrayList<>(groupByFieldNames);
        outputFields.add(outputField);
        return groupedStream
                .chainedAgg()
                .aggregate(new Count(), new Fields("__cnt__"))
                .aggregate(new Fields(inputFields), new SumBy(inputFieldName), new Fields("__sum__"))
                .chainEnd()
                .each(new Fields("__sum__", "__cnt__"), new DivideAsDouble(), new Fields(outputField))
                .project(new Fields(outputFields))
                .name(call.toString());
    }

    private Stream handleSumFunction(GroupedStream groupedStream, List<String> groupByFieldNames, Stream inputStream, AggregateCall call, String outputField) {
        List<String> inputFields = new ArrayList<>(groupByFieldNames);
        Integer fieldIdx = call.getArgList().get(0);
        String inputFieldName = inputStream.getOutputFields().get(fieldIdx);
        inputFields.add(inputFieldName);

        List<String> outputFields = new ArrayList<>(groupByFieldNames);
        outputFields.add(outputField);
        return groupedStream
                .aggregate(new Fields(inputFields), new SumBy(inputFieldName), new Fields(outputField))
                .project(new Fields(outputFields))
                .name(call.toString());
    }

    private Stream handleMinFunction(GroupedStream groupedStream, List<String> groupByFieldNames, Stream inputStream, AggregateCall call, String outputField) {
        List<String> inputFields = new ArrayList<>(groupByFieldNames);
        Integer fieldIdx = call.getArgList().get(0);
        String inputFieldName = inputStream.getOutputFields().get(fieldIdx);
        inputFields.add(inputFieldName);

        List<String> outputFields = new ArrayList<>(groupByFieldNames);
        outputFields.add(outputField);
        return groupedStream
                .aggregate(new Fields(inputFields), new MinBy(inputFieldName), new Fields(outputField))
                .project(new Fields(outputFields))
                .name(call.toString());
    }

    private Stream handleMaxFunction(GroupedStream groupedStream, List<String> groupByFieldNames, Stream inputStream, AggregateCall call, String outputField) {
        List<String> inputFields = new ArrayList<>(groupByFieldNames);
        Integer fieldIdx = call.getArgList().get(0);
        String inputFieldName = inputStream.getOutputFields().get(fieldIdx);
        inputFields.add(inputFieldName);

        List<String> outputFields = new ArrayList<>(groupByFieldNames);
        outputFields.add(outputField);
        return groupedStream
                .aggregate(new Fields(inputFields), new MaxBy(inputFieldName), new Fields(outputField))
                .project(new Fields(outputFields))
                .name(call.toString());
    }

    private Stream handleCountFunction(GroupedStream groupedStream, List<String> groupByFieldNames, Stream inputStream, AggregateCall call, String outputField) {
        List<String> inputFields = new ArrayList<>(groupByFieldNames);
        if (call.getArgList().size() == 0) {
            return groupedStream.aggregate(new Fields(inputFields), new Count(), new Fields(outputField)).name(call.toString());
        } else {
            // call.getArgList().size() == 1
            Integer fieldIdx = call.getArgList().get(0);
            String inputFieldName = inputStream.getOutputFields().get(fieldIdx);
            return groupedStream.aggregate(new Fields(inputFields), new CountBy(inputFieldName), new Fields(outputField));
        }
    }

    private String getStageName(RelNode n) {
        return n.getClass().getSimpleName().toUpperCase() + "_" + n.getId();
    }
}