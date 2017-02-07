/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.storm.sql.planner.trident;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
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
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.sql.planner.StormRelDataTypeSystem;
import org.apache.storm.sql.planner.UnsupportedOperatorsVisitor;
import org.apache.storm.sql.planner.trident.rel.TridentLogicalConvention;
import org.apache.storm.sql.planner.trident.rel.TridentRel;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.sql.AbstractTridentProcessor;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.IAggregatableStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
                false,
                Collections.<String>emptyList(), typeFactory));

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables))
                .traitDefs(traitDefs)
                .context(Contexts.EMPTY_CONTEXT)
                .ruleSets(TridentStormRuleSets.getRuleSets())
                .costFactory(null)
                .typeSystem(StormRelDataTypeSystem.STORM_REL_DATATYPE_SYSTEM)
                .build();
        this.planner = Frameworks.getPlanner(config);
    }

    public AbstractTridentProcessor compile(Map<String, ISqlTridentDataSource> sources, String query) throws Exception {
        TridentRel relNode = getPlan(query);

        TridentPlanCreator tridentPlanCreator = new TridentPlanCreator(sources, new RexBuilder(typeFactory));
        relNode.tridentPlan(tridentPlanCreator);

        final TridentTopology topology = tridentPlanCreator.getTopology();
        final IAggregatableStream lastStream = tridentPlanCreator.pop();
        final DataContext dc = tridentPlanCreator.getDataContext();
        final List<CompilingClassLoader> cls = tridentPlanCreator.getClassLoaders();

        return new AbstractTridentProcessor() {
            @Override
            public TridentTopology build() {
                return topology;
            }

            @Override
            public Stream outputStream() {
                return lastStream.toStream();
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

    public TridentRel getPlan(String query) throws ValidationException, RelConversionException, SqlParseException {
        return (TridentRel) validateAndConvert(planner.parse(query));
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
        return planner.transform(STORM_REL_CONVERSION_RULES, traitSet.plus(TridentLogicalConvention.INSTANCE), relNode);
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
