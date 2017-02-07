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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.storm.sql.planner.StormRelUtils;
import org.apache.storm.sql.planner.rel.StormProjectRelBase;
import org.apache.storm.sql.planner.trident.TridentPlanCreator;
import org.apache.storm.sql.runtime.calcite.ExecutableExpression;
import org.apache.storm.sql.runtime.trident.functions.EvaluationFunction;
import org.apache.storm.trident.Stream;
import org.apache.storm.tuple.Fields;

import java.util.List;

public class TridentProjectRel extends StormProjectRelBase implements TridentRel {
    public TridentProjectRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new TridentProjectRel(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public void tridentPlan(TridentPlanCreator planCreator) throws Exception {
        // SingleRel
        RelNode input = getInput();
        StormRelUtils.getStormRelInput(input).tridentPlan(planCreator);
        Stream inputStream = planCreator.pop().toStream();

        String stageName = StormRelUtils.getStageName(this);
        String projectionClassName = StormRelUtils.getClassName(this);

        List<String> outputFieldNames = getRowType().getFieldNames();
        int outputCount = outputFieldNames.size();

        List<RexNode> childExps = getChildExps();
        RelDataType inputRowType = getInput(0).getRowType();

        ExecutableExpression projectionInstance = planCreator.createScalarInstance(childExps, inputRowType, projectionClassName);
        Stream finalStream = inputStream
                .map(new EvaluationFunction(projectionInstance, outputCount, planCreator.getDataContext()), new Fields(outputFieldNames))
                .name(stageName);

        planCreator.addStream(finalStream);
    }
}
