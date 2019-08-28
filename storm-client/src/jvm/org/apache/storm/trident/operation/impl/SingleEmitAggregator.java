/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.trident.operation.impl;

import java.io.Serializable;
import java.util.Map;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.operation.impl.SingleEmitAggregator.SingleEmitState;
import org.apache.storm.trident.tuple.TridentTuple;


public class SingleEmitAggregator implements Aggregator<SingleEmitState> {
    Aggregator agg;
    BatchToPartition batchToPartition;
    int myPartitionIndex;
    int totalPartitions;

    public SingleEmitAggregator(Aggregator agg, BatchToPartition batchToPartition) {
        this.agg = agg;
        this.batchToPartition = batchToPartition;
    }


    @Override
    public SingleEmitState init(Object batchId, TridentCollector collector) {
        return new SingleEmitState(batchId);
    }

    @Override
    public void aggregate(SingleEmitState val, TridentTuple tuple, TridentCollector collector) {
        if (!val.received) {
            val.state = agg.init(val.batchId, collector);
            val.received = true;
        }
        agg.aggregate(val.state, tuple, collector);
    }

    @Override
    public void complete(SingleEmitState val, TridentCollector collector) {
        if (!val.received) {
            if (this.myPartitionIndex == batchToPartition.partitionIndex(val.batchId, this.totalPartitions)) {
                val.state = agg.init(val.batchId, collector);
                agg.complete(val.state, collector);
            }
        } else {
            agg.complete(val.state, collector);
        }
    }

    @Override
    public void prepare(Map<String, Object> conf, TridentOperationContext context) {
        agg.prepare(conf, context);
        this.myPartitionIndex = context.getPartitionIndex();
        this.totalPartitions = context.numPartitions();
    }

    @Override
    public void cleanup() {
        agg.cleanup();
    }

    public interface BatchToPartition extends Serializable {
        int partitionIndex(Object batchId, int numPartitions);
    }

    static class SingleEmitState {
        boolean received = false;
        Object state;
        Object batchId;

        SingleEmitState(Object batchId) {
            this.batchId = batchId;
        }
    }


}
