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
package org.apache.storm.sql.planner.trident.rel;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.storm.sql.planner.StormRelUtils;
import org.apache.storm.sql.planner.rel.StormStreamInsertRelBase;
import org.apache.storm.sql.planner.trident.TridentPlanCreator;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.fluent.IAggregatableStream;
import org.apache.storm.tuple.Fields;

import java.util.List;

public class TridentStreamInsertRel extends StormStreamInsertRelBase implements TridentRel {
    public TridentStreamInsertRel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened) {
        super(cluster, traits, table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new TridentStreamInsertRel(getCluster(), traitSet, getTable(), getCatalogReader(),
                sole(inputs), getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened());
    }

    @Override
    public void tridentPlan(TridentPlanCreator planCreator) throws Exception {
        // SingleRel
        RelNode input = getInput();
        StormRelUtils.getStormRelInput(input).tridentPlan(planCreator);
        Stream inputStream = planCreator.pop().toStream();

        String stageName = StormRelUtils.getStageName(this);

        Preconditions.checkArgument(isInsert(), "Only INSERT statement is supported.");

        List<String> inputFields = this.input.getRowType().getFieldNames();
        List<String> outputFields = getRowType().getFieldNames();

        // FIXME: this should be really different...
        String tableName = Joiner.on('.').join(getTable().getQualifiedName());
        ISqlTridentDataSource.SqlTridentConsumer consumer = planCreator.getSources().get(tableName).getConsumer();

        // In fact this is normally the end of stream, but I'm still not sure so I open new streams based on State values
        IAggregatableStream finalStream = inputStream
                .partitionPersist(consumer.getStateFactory(), new Fields(inputFields), consumer.getStateUpdater(),
                        new Fields(outputFields))
                .newValuesStream().name(stageName);

        planCreator.addStream(finalStream);
    }

}
