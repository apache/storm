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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.runtime.trident.AbstractTridentProcessor;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.IAggregatableStream;

import java.util.Map;

public class PlanCompiler {
    private Map<String, ISqlTridentDataSource> sources;
    private final JavaTypeFactory typeFactory;
    private final DataContext dataContext;

    public PlanCompiler(Map<String, ISqlTridentDataSource> sources, JavaTypeFactory typeFactory, DataContext dataContext) {
        this.sources = sources;
        this.typeFactory = typeFactory;
        this.dataContext = dataContext;
    }

    public AbstractTridentProcessor compileForTest(RelNode plan) throws Exception {
        final TridentTopology topology = new TridentTopology();

        TridentLogicalPlanCompiler compiler = new TridentLogicalPlanCompiler(sources, typeFactory, topology, dataContext);
        final IAggregatableStream stream = compiler.traverse(plan);

        return new AbstractTridentProcessor() {
            @Override
            public Stream outputStream() {
                return stream.toStream();
            }

            @Override
            public TridentTopology build(Map<String, ISqlTridentDataSource> sources) {
                return topology;
            }
        };
    }

    public TridentTopology compile(RelNode plan) throws Exception {
        TridentTopology topology = new TridentTopology();

        TridentLogicalPlanCompiler compiler = new TridentLogicalPlanCompiler(sources, typeFactory, topology, dataContext);
        compiler.traverse(plan);

        return topology;
    }




}
