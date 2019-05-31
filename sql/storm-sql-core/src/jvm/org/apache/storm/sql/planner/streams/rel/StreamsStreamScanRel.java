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
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.storm.sql.planner.rel.StormStreamScanRelBase;
import org.apache.storm.sql.planner.streams.StreamsPlanCreator;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.sql.runtime.streams.functions.StreamsScanTupleValueMapper;
import org.apache.storm.streams.Stream;
import org.apache.storm.tuple.Values;

public class StreamsStreamScanRel extends StormStreamScanRelBase implements StreamsRel {
    private final int parallelismHint;

    public StreamsStreamScanRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, int parallelismHint) {
        super(cluster, traitSet, table);
        this.parallelismHint = parallelismHint;
    }

    @Override
    public void streamsPlan(StreamsPlanCreator planCreator) throws Exception {
        String sourceName = Joiner.on('.').join(getTable().getQualifiedName());

        Map<String, ISqlStreamsDataSource> sources = planCreator.getSources();
        if (!sources.containsKey(sourceName)) {
            throw new RuntimeException("Cannot find table " + sourceName);
        }

        List<String> fieldNames = getRowType().getFieldNames();
        final Stream<Values> finalStream = planCreator.getStreamBuilder()
                .newStream(sources.get(sourceName).getProducer(), new StreamsScanTupleValueMapper(fieldNames), parallelismHint);
        planCreator.addStream(finalStream);
    }
}
