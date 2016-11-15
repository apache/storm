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
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.storm.sql.compiler.PostOrderRelNodeVisitor;
import org.apache.storm.sql.compiler.RexNodeToBlockStatementCompiler;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.trident.functions.EvaluationFilter;
import org.apache.storm.sql.runtime.trident.functions.EvaluationFunction;
import org.apache.storm.sql.runtime.trident.functions.ForwardFunction;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.IAggregatableStream;
import org.apache.storm.tuple.Fields;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TridentLogicalPlanCompiler extends PostOrderRelNodeVisitor<IAggregatableStream> {
    protected final Map<String, ISqlTridentDataSource> sources;
    protected final JavaTypeFactory typeFactory;
    protected final RexNodeToBlockStatementCompiler rexCompiler;
    private final DataContext dataContext;
    protected TridentTopology topology;

    public TridentLogicalPlanCompiler(Map<String, ISqlTridentDataSource> sources, JavaTypeFactory typeFactory,
                                      TridentTopology topology, DataContext dataContext) {
        this.sources = sources;
        this.typeFactory = typeFactory;
        this.topology = topology;
        this.rexCompiler = new RexNodeToBlockStatementCompiler(new RexBuilder(typeFactory));
        this.dataContext = dataContext;
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
    public IAggregatableStream visitProject(Project project, List<IAggregatableStream> inputStreams) throws Exception {
        if (inputStreams.size() != 1) {
            throw new RuntimeException("Project is a SingleRel");
        }

        Stream inputStream = inputStreams.get(0).toStream();
        Fields inputFields = inputStream.getOutputFields();
        String stageName = getStageName(project);

        // Trident doesn't allow duplicated field name... need to do the trick...
        List<String> outputFieldNames = project.getRowType().getFieldNames();
        int outputCount = outputFieldNames.size();
        List<String> temporaryOutputFieldNames = new ArrayList<>();
        for (String outputFieldName : outputFieldNames) {
            temporaryOutputFieldNames.add("__" + outputFieldName + "__");
        }

        try (StringWriter sw = new StringWriter()) {
            List<RexNode> childExps = project.getChildExps();
            RelDataType inputRowType = project.getInput(0).getRowType();
            BlockStatement codeBlock = rexCompiler.compile(childExps, inputRowType);
            sw.write(codeBlock.toString());

            // we use out parameter and don't use the return value but compile fails...
            sw.write("return 0;");

            final String expression = sw.toString();

            return inputStream.each(inputFields, new EvaluationFunction(expression, outputCount, dataContext), new Fields(temporaryOutputFieldNames))
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
            List<RexNode> childExps = filter.getChildExps();
            RelDataType inputRowType = filter.getInput(0).getRowType();
            BlockStatement codeBlock = rexCompiler.compile(childExps, inputRowType);
            sw.write(codeBlock.toString());

            // we use out parameter and don't use the return value but compile fails...
            sw.write("return 0;");

            final String expression = sw.toString();

            return inputStream.filter(new EvaluationFilter(expression, dataContext)).name(stageName);
        }
    }

    private String getStageName(RelNode n) {
        return n.getClass().getSimpleName().toUpperCase() + "_" + n.getId();
    }
}