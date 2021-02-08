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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.storm.sql.planner.streams.StreamsPlanCreator;
import org.apache.storm.sql.runtime.calcite.ExecutableExpression;
import org.apache.storm.sql.runtime.streams.functions.EvaluationCalc;
import org.apache.storm.streams.Stream;
import org.apache.storm.tuple.Values;

public class StreamsCalcRel extends StormCalcRelBase implements StreamsRel {
    public StreamsCalcRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexProgram program) {
        super(cluster, traits, child, program);
    }

    @Override
    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        return new StreamsCalcRel(getCluster(), traitSet, child, program);
    }

    @Override
    public void streamsPlan(StreamsPlanCreator planCreator) throws Exception {
        // SingleRel
        RelNode input = getInput();
        StormRelUtils.getStormRelInput(input).streamsPlan(planCreator);

        RelDataType inputRowType = getInput(0).getRowType();

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

        List<String> outputFieldNames = getRowType().getFieldNames();
        int outputCount = outputFieldNames.size();
        EvaluationCalc evalCalc = new EvaluationCalc(filterInstance, projectionInstance, outputCount, planCreator.getDataContext());
        final Stream<Values> inputStream = planCreator.pop();
        final Stream finalStream = inputStream.flatMap(evalCalc);

        planCreator.addStream(finalStream);
    }
}
