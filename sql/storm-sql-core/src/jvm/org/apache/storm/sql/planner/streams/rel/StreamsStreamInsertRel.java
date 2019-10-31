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

package org.apache.storm.sql.planner.streams.rel;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.storm.sql.planner.StormRelUtils;
import org.apache.storm.sql.planner.rel.StormStreamInsertRelBase;
import org.apache.storm.sql.planner.streams.StreamsPlanCreator;
import org.apache.storm.sql.runtime.streams.functions.StreamInsertMapToPairFunction;
import org.apache.storm.streams.Stream;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Values;

public class StreamsStreamInsertRel extends StormStreamInsertRelBase implements StreamsRel {
    private final int primaryKeyIndex;

    public StreamsStreamInsertRel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, Prepare.CatalogReader catalogReader,
                                  RelNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList,
                                  boolean flattened, int primaryKeyIndex) {
        super(cluster, traits, table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened);
        this.primaryKeyIndex = primaryKeyIndex;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamsStreamInsertRel(getCluster(), traitSet, getTable(), getCatalogReader(),
                sole(inputs), getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened(), primaryKeyIndex);
    }

    @Override
    public void streamsPlan(StreamsPlanCreator planCreator) throws Exception {
        // SingleRel
        RelNode input = getInput();
        StormRelUtils.getStormRelInput(input).streamsPlan(planCreator);
        Stream<Values> inputStream = planCreator.pop();

        Preconditions.checkArgument(isInsert(), "Only INSERT statement is supported.");

        // Calcite ensures that the value is structurized to the table definition
        // hence we can use PK index directly
        // To elaborate, if table BAR is defined as ID INTEGER PK, NAME VARCHAR, DEPTID INTEGER
        // and query like INSERT INTO BAR SELECT NAME, ID FROM FOO is executed,
        // Calcite makes the projection ($1 <- ID, $0 <- NAME, null) to the value before INSERT.

        String tableName = Joiner.on('.').join(getTable().getQualifiedName());
        IRichBolt consumer = planCreator.getSources().get(tableName).getConsumer();

        // To make logic simple, it assumes that all the tables have one PK (which it should be extended to support composed key),
        // and provides PairStream(KeyedStream) to consumer bolt.
        inputStream.mapToPair(new StreamInsertMapToPairFunction(primaryKeyIndex)).to(consumer);

        planCreator.addStream(inputStream);
    }

}
