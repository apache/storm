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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.storm.sql.planner.streams.rel.StreamsLogicalConvention;
import org.apache.storm.sql.planner.streams.rel.StreamsProjectRel;

public class StreamsProjectRule extends ConverterRule {
    public static final StreamsProjectRule INSTANCE = new StreamsProjectRule();

    private StreamsProjectRule() {
        super(LogicalProject.class, Convention.NONE, StreamsLogicalConvention.INSTANCE,
              "StreamsProjectRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final Project project = (Project) rel;
        final RelNode input = project.getInput();

        return new StreamsProjectRel(project.getCluster(),
                                     project.getTraitSet().replace(StreamsLogicalConvention.INSTANCE),
                                     convert(input, input.getTraitSet().replace(StreamsLogicalConvention.INSTANCE)), project.getProjects(),
                                     project.getRowType());
    }
}
