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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.storm.sql.planner.StormRelUtils;
import org.apache.storm.sql.planner.rel.StormFilterRelBase;
import org.apache.storm.sql.planner.streams.StreamsPlanCreator;
import org.apache.storm.sql.runtime.calcite.ExecutableExpression;
import org.apache.storm.sql.runtime.streams.functions.EvaluationFilter;
import org.apache.storm.streams.Stream;
import org.apache.storm.tuple.Values;

public class StreamsFilterRel extends StormFilterRelBase implements StreamsRel {
    public StreamsFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new StreamsFilterRel(getCluster(), traitSet, input, condition);
    }

    @Override
    public void streamsPlan(StreamsPlanCreator planCreator) throws Exception {
        // SingleRel
        RelNode input = getInput();
        StormRelUtils.getStormRelInput(input).streamsPlan(planCreator);
        Stream<Values> inputStream = planCreator.pop();

        List<RexNode> childExps = getChildExps();
        RelDataType inputRowType = getInput(0).getRowType();

        String filterClassName = StormRelUtils.getClassName(this);
        ExecutableExpression filterInstance = planCreator.createScalarInstance(childExps, inputRowType, filterClassName);

        EvaluationFilter evalFilter = new EvaluationFilter(filterInstance, planCreator.getDataContext());
        final Stream<Values> finalStream = inputStream.filter(evalFilter);

        planCreator.addStream(finalStream);
    }
}
