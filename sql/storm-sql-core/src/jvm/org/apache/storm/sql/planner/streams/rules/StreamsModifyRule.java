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

package org.apache.storm.sql.planner.streams.rules;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.storm.sql.calcite.StormStreamableTable;
import org.apache.storm.sql.calcite.StormTable;
import org.apache.storm.sql.planner.streams.rel.StreamsLogicalConvention;
import org.apache.storm.sql.planner.streams.rel.StreamsStreamInsertRel;

public class StreamsModifyRule extends ConverterRule {
    public static final StreamsModifyRule INSTANCE = new StreamsModifyRule();

    private StreamsModifyRule() {
        super(LogicalTableModify.class, Convention.NONE, StreamsLogicalConvention.INSTANCE, "StreamsModifyRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final TableModify tableModify = (TableModify) rel;
        final RelNode input = tableModify.getInput();

        final RelOptCluster cluster = tableModify.getCluster();
        final RelTraitSet traitSet = tableModify.getTraitSet().replace(StreamsLogicalConvention.INSTANCE);
        final RelOptTable relOptTable = tableModify.getTable();
        final Prepare.CatalogReader catalogReader = tableModify.getCatalogReader();
        final RelNode convertedInput = convert(input, input.getTraitSet().replace(StreamsLogicalConvention.INSTANCE));
        final TableModify.Operation operation = tableModify.getOperation();
        final List<String> updateColumnList = tableModify.getUpdateColumnList();
        final List<RexNode> sourceExpressionList = tableModify.getSourceExpressionList();
        final boolean flattened = tableModify.isFlattened();

        int primaryKey;

        StormTable stormTable = tableModify.getTable().unwrap(StormTable.class);
        if (stormTable != null) {
            primaryKey = stormTable.primaryKey();
        } else {
            StormStreamableTable streamableTable = tableModify.getTable().unwrap(StormStreamableTable.class);
            if (streamableTable != null) {
                primaryKey = streamableTable.primaryKey();
            } else {
                throw new IllegalStateException("Table must be able to unwrap with StormTable or StormStreamableTable.");
            }
        }

        final Table table = tableModify.getTable().unwrap(Table.class);

        switch (table.getJdbcTableType()) {
            case STREAM:
                if (operation != TableModify.Operation.INSERT) {
                    throw new UnsupportedOperationException(String.format("Stream doesn't support %s modify operation", operation));
                }
                return new StreamsStreamInsertRel(cluster, traitSet, relOptTable, catalogReader, convertedInput, operation,
                        updateColumnList, sourceExpressionList, flattened, primaryKey);
            default:
                throw new IllegalArgumentException(String.format("Unsupported table type: %s", table.getJdbcTableType()));
        }
    }
}
