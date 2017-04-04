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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.storm.sql.planner.StormRelUtils;
import org.apache.storm.sql.planner.rel.StormStreamScanRelBase;
import org.apache.storm.sql.planner.trident.TridentPlanCreator;
import org.apache.storm.sql.runtime.ISqlTridentDataSource;
import org.apache.storm.trident.fluent.IAggregatableStream;

import java.util.Map;

public class TridentStreamScanRel extends StormStreamScanRelBase implements TridentRel {
    private final int parallelismHint;

    public TridentStreamScanRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, int parallelismHint) {
        super(cluster, traitSet, table);
        this.parallelismHint = parallelismHint;
    }

    @Override
    public void tridentPlan(TridentPlanCreator planCreator) throws Exception {
        String sourceName = Joiner.on('.').join(getTable().getQualifiedName());

        // FIXME: this should be really different...
        Map<String, ISqlTridentDataSource> sources = planCreator.getSources();
        if (!sources.containsKey(sourceName)) {
            throw new RuntimeException("Cannot find table " + sourceName);
        }

        String stageName = StormRelUtils.getStageName(this);
        IAggregatableStream finalStream = planCreator.getTopology().newStream(stageName, sources.get(sourceName).getProducer())
                .parallelismHint(parallelismHint);
        planCreator.addStream(finalStream);
    }
}
