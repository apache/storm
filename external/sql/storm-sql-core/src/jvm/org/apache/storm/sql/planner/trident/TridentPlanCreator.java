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
package org.apache.storm.sql.planner.trident;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.storm.sql.compiler.RexNodeToBlockStatementCompiler;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.calcite.StormDataContext;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.IAggregatableStream;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

public class TridentPlanCreator {
    private final Map<String, ISqlTridentDataSource> sources;
    private final JavaTypeFactory typeFactory;
    private final RexNodeToBlockStatementCompiler rexCompiler;
    private final DataContext dataContext;
    private final TridentTopology topology;

    private final Deque<IAggregatableStream> streamStack = new ArrayDeque<>();

    public TridentPlanCreator(Map<String, ISqlTridentDataSource> sources, RexBuilder rexBuilder) {
        this.sources = sources;
        this.rexCompiler = new RexNodeToBlockStatementCompiler(rexBuilder);
        this.typeFactory = (JavaTypeFactory) rexBuilder.getTypeFactory();

        this.topology = new TridentTopology();
        this.dataContext = new StormDataContext();
    }

    public void addStream(IAggregatableStream stream) throws Exception {
        push(stream);
    }

    public IAggregatableStream pop() {
        return streamStack.pop();
    }

    public Map<String, ISqlTridentDataSource> getSources() {
        return sources;
    }

    public DataContext getDataContext() {
        return dataContext;
    }

    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public TridentTopology getTopology() {
        return topology;
    }

    public String createExpression(List<RexNode> nodes, RelDataType inputRowType) {
        return rexCompiler.compile(nodes, inputRowType).toString();
    }

    public String createExpression(RexProgram program) {
        return rexCompiler.compile(program).toString();
    }

    private void push(IAggregatableStream stream) {
        streamStack.push(stream);
    }
}
