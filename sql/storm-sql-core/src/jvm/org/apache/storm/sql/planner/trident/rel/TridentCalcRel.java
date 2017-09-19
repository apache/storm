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

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.storm.sql.planner.StormRelUtils;
import org.apache.storm.sql.planner.rel.StormCalcRelBase;
import org.apache.storm.sql.planner.trident.TridentPlanCreator;
import org.apache.storm.sql.runtime.calcite.ExecutableExpression;
import org.apache.storm.sql.runtime.trident.functions.EvaluationCalc;
import org.apache.storm.trident.Stream;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

public class TridentCalcRel extends StormCalcRelBase implements TridentRel {
    public TridentCalcRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexProgram program) {
        super(cluster, traits, child, program);
    }

    @Override
    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        return new TridentCalcRel(getCluster(), traitSet, child, program);
    }

    @Override
    public void tridentPlan(TridentPlanCreator planCreator) throws Exception {
        // SingleRel
        RelNode input = getInput();
        StormRelUtils.getStormRelInput(input).tridentPlan(planCreator);
        Stream inputStream = planCreator.pop().toStream();

        String stageName = StormRelUtils.getStageName(this);

        RelDataType inputRowType = getInput(0).getRowType();

        List<String> outputFieldNames = getRowType().getFieldNames();
        int outputCount = outputFieldNames.size();

        // filter
        ExecutableExpression filterInstance = null;
        RexLocalRef condition = program.getCondition();
        if (condition != null) {
            RexNode conditionNode = program.expandLocalRef(condition);
            filterInstance = planCreator.createScalarInstance(Lists.newArrayList(conditionNode), inputRowType,
                    StormRelUtils.getClassName(this));
        }

        // projection
        ExecutableExpression projectionInstance = null;
        List<RexLocalRef> projectList = program.getProjectList();
        if (projectList != null && !projectList.isEmpty()) {
            List<RexNode> expandedNodes = new ArrayList<>();
            for (RexLocalRef project : projectList) {
                expandedNodes.add(program.expandLocalRef(project));
            }

            projectionInstance = planCreator.createScalarInstance(expandedNodes, inputRowType,
                    StormRelUtils.getClassName(this));
        }

        if (projectionInstance == null && filterInstance == null) {
            // it shouldn't be happen
            throw new IllegalStateException("Either projection or condition, or both should be provided.");
        }

        final Stream finalStream = inputStream
                .flatMap(new EvaluationCalc(filterInstance, projectionInstance, outputCount, planCreator.getDataContext()), new Fields(outputFieldNames))
                .name(stageName);

        planCreator.addStream(finalStream);
    }
}
