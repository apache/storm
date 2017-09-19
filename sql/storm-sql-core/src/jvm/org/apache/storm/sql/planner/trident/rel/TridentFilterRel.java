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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.storm.sql.planner.StormRelUtils;
import org.apache.storm.sql.planner.rel.StormFilterRelBase;
import org.apache.storm.sql.planner.trident.TridentPlanCreator;
import org.apache.storm.sql.runtime.calcite.ExecutableExpression;
import org.apache.storm.sql.runtime.trident.functions.EvaluationFilter;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.fluent.IAggregatableStream;

import java.util.List;

public class TridentFilterRel extends StormFilterRelBase implements TridentRel {
    public TridentFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new TridentFilterRel(getCluster(), traitSet, input, condition);
    }

    @Override
    public void tridentPlan(TridentPlanCreator planCreator) throws Exception {
        // SingleRel
        RelNode input = getInput();
        StormRelUtils.getStormRelInput(input).tridentPlan(planCreator);
        Stream inputStream = planCreator.pop().toStream();

        String stageName = StormRelUtils.getStageName(this);

        List<RexNode> childExps = getChildExps();
        RelDataType inputRowType = getInput(0).getRowType();

        String filterClassName = StormRelUtils.getClassName(this);
        ExecutableExpression filterInstance = planCreator.createScalarInstance(childExps, inputRowType, filterClassName);

        IAggregatableStream finalStream = inputStream.filter(new EvaluationFilter(filterInstance, planCreator.getDataContext()))
                .name(stageName);
        planCreator.addStream(finalStream);
    }
}
