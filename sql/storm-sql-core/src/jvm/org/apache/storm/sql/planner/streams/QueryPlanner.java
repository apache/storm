/*
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
 *
 */

package org.apache.storm.sql.planner.streams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.sql.AbstractStreamsProcessor;
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.sql.planner.StormRelDataTypeSystem;
import org.apache.storm.sql.planner.UnsupportedOperatorsVisitor;
import org.apache.storm.sql.planner.streams.rel.StreamsLogicalConvention;
import org.apache.storm.sql.planner.streams.rel.StreamsRel;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.tuple.Values;

public class QueryPlanner {

    public static final int STORM_REL_CONVERSION_RULES = 1;

    private final Planner planner;

    private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(
        RelDataTypeSystem.DEFAULT);

    public QueryPlanner(SchemaPlus schema) {
        final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);

        List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
        sqlOperatorTables.add(SqlStdOperatorTable.instance());
        sqlOperatorTables.add(new CalciteCatalogReader(CalciteSchema.from(schema),
                Collections.emptyList(), typeFactory, new CalciteConnectionConfigImpl(new Properties())));

        FrameworkConfig config = Frameworks.newConfigBuilder()
                                           .defaultSchema(schema)
                                           .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables))
                                           .traitDefs(traitDefs)
                                           .context(Contexts.EMPTY_CONTEXT)
                                           .ruleSets(StreamsStormRuleSets.getRuleSets())
                                           .costFactory(null)
                                           .typeSystem(StormRelDataTypeSystem.STORM_REL_DATATYPE_SYSTEM)
                                           .build();
        this.planner = Frameworks.getPlanner(config);
    }

    public AbstractStreamsProcessor compile(Map<String, ISqlStreamsDataSource> sources, String query) throws Exception {
        StreamsRel relNode = getPlan(query);

        StreamsPlanCreator streamsPlanCreator = new StreamsPlanCreator(sources, new RexBuilder(typeFactory));
        relNode.streamsPlan(streamsPlanCreator);

        final StreamBuilder streamBuilder = streamsPlanCreator.getStreamBuilder();
        final Stream<Values> lastStream = streamsPlanCreator.pop();
        final DataContext dc = streamsPlanCreator.getDataContext();
        final List<CompilingClassLoader> cls = streamsPlanCreator.getClassLoaders();

        return new AbstractStreamsProcessor() {
            @Override
            public StormTopology build() {
                return streamBuilder.build();
            }

            @Override
            public Stream<Values> outputStream() {
                return lastStream;
            }

            @Override
            public DataContext getDataContext() {
                return dc;
            }

            @Override
            public List<CompilingClassLoader> getClassLoaders() {
                return cls;
            }
        };
    }

    public StreamsRel getPlan(String query) throws ValidationException, RelConversionException, SqlParseException {
        return (StreamsRel) validateAndConvert(planner.parse(query));
    }

    private RelNode validateAndConvert(SqlNode sqlNode) throws ValidationException, RelConversionException {
        SqlNode validated = validateNode(sqlNode);
        RelNode relNode = convertToRelNode(validated);
        return convertToStormRel(relNode);
    }

    private RelNode convertToStormRel(RelNode relNode) throws RelConversionException {
        RelTraitSet traitSet = relNode.getTraitSet();
        traitSet = traitSet.simplify();

        // PlannerImpl.transform() optimizes RelNode with ruleset
        return planner.transform(STORM_REL_CONVERSION_RULES, traitSet.plus(StreamsLogicalConvention.INSTANCE), relNode);
    }

    private RelNode convertToRelNode(SqlNode sqlNode) throws RelConversionException {
        return planner.rel(sqlNode).rel;
    }

    private SqlNode validateNode(SqlNode sqlNode) throws ValidationException {
        SqlNode validatedSqlNode = planner.validate(sqlNode);
        validatedSqlNode.accept(new UnsupportedOperatorsVisitor());
        return validatedSqlNode;
    }
}
