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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.Table;
import org.apache.storm.sql.calcite.ParallelStreamableTable;
import org.apache.storm.sql.calcite.ParallelTable;
import org.apache.storm.sql.planner.streams.rel.StreamsLogicalConvention;
import org.apache.storm.sql.planner.streams.rel.StreamsStreamScanRel;

public class StreamsScanRule extends ConverterRule {
    public static final StreamsScanRule INSTANCE = new StreamsScanRule();
    public static final int DEFAULT_PARALLELISM_HINT = 1;

    private StreamsScanRule() {
        super(EnumerableTableScan.class, EnumerableConvention.INSTANCE, StreamsLogicalConvention.INSTANCE, "StreamsScanRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final TableScan scan = (TableScan) rel;
        int parallelismHint = DEFAULT_PARALLELISM_HINT;

        final ParallelTable parallelTable = scan.getTable().unwrap(ParallelTable.class);
        if (parallelTable != null && parallelTable.parallelismHint() != null) {
            parallelismHint = parallelTable.parallelismHint();
        }

        final Table table = scan.getTable().unwrap(Table.class);
        switch (table.getJdbcTableType()) {
            case STREAM:
                return new StreamsStreamScanRel(scan.getCluster(),
                                                scan.getTraitSet().replace(StreamsLogicalConvention.INSTANCE),
                                                scan.getTable(), parallelismHint);
            default:
                throw new IllegalArgumentException(String.format("Unsupported table type: %s", table.getJdbcTableType()));
        }
    }
}
