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

package org.apache.storm.trident.fluent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.impl.ChainedAggregatorImpl;
import org.apache.storm.trident.operation.impl.CombinerAggregatorCombineImpl;
import org.apache.storm.trident.operation.impl.CombinerAggregatorInitImpl;
import org.apache.storm.trident.operation.impl.ReducerAggregatorImpl;
import org.apache.storm.trident.operation.impl.SingleEmitAggregator;
import org.apache.storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;
import org.apache.storm.trident.tuple.ComboList;
import org.apache.storm.tuple.Fields;


public class ChainedAggregatorDeclarer implements ChainedFullAggregatorDeclarer, ChainedPartitionAggregatorDeclarer {
    List<AggSpec> aggs = new ArrayList<>();
    IAggregatableStream stream;
    AggType type = null;
    GlobalAggregationScheme globalScheme;

    public ChainedAggregatorDeclarer(IAggregatableStream stream, GlobalAggregationScheme globalScheme) {
        this.stream = stream;
        this.globalScheme = globalScheme;
    }

    @Override
    public Stream chainEnd() {
        Fields[] inputFields = new Fields[aggs.size()];
        Aggregator[] aggs = new Aggregator[this.aggs.size()];
        int[] outSizes = new int[this.aggs.size()];
        List<String> allOutFields = new ArrayList<>();
        Set<String> allInFields = new HashSet<>();
        for (int i = 0; i < this.aggs.size(); i++) {
            AggSpec spec = this.aggs.get(i);
            Fields infields = spec.inFields;
            if (infields == null) {
                infields = new Fields();
            }
            Fields outfields = spec.outFields;
            if (outfields == null) {
                outfields = new Fields();
            }

            inputFields[i] = infields;
            aggs[i] = spec.agg;
            outSizes[i] = outfields.size();
            allOutFields.addAll(outfields.toList());
            allInFields.addAll(infields.toList());
        }
        if (new HashSet(allOutFields).size() != allOutFields.size()) {
            throw new IllegalArgumentException("Output fields for chained aggregators must be distinct: " + allOutFields.toString());
        }

        Fields inFields = new Fields(new ArrayList<>(allInFields));
        Fields outFields = new Fields(allOutFields);
        Aggregator combined = new ChainedAggregatorImpl(aggs, inputFields, new ComboList.Factory(outSizes));

        if (type != AggType.FULL) {
            stream = stream.partitionAggregate(inFields, combined, outFields);
        }
        if (type != AggType.PARTITION) {
            stream = globalScheme.aggPartition(stream);
            BatchToPartition singleEmit = globalScheme.singleEmitPartitioner();
            Aggregator toAgg = combined;
            if (singleEmit != null) {
                toAgg = new SingleEmitAggregator(combined, singleEmit);
            }
            // this assumes that inFields and outFields are the same for combineragg
            // assumption also made above
            stream = stream.partitionAggregate(inFields, toAgg, outFields);
        }
        return stream.toStream();
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(Aggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        type = AggType.PARTITION;
        aggs.add(new AggSpec(inputFields, agg, functionFields));
        return this;
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(CombinerAggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        initCombiner(inputFields, agg, functionFields);
        return partitionAggregate(functionFields, new CombinerAggregatorCombineImpl(agg), functionFields);
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(ReducerAggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    @Override
    public ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return partitionAggregate(inputFields, new ReducerAggregatorImpl(agg), functionFields);
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(Aggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        return aggregate(inputFields, agg, functionFields, false);
    }

    private ChainedFullAggregatorDeclarer aggregate(Fields inputFields, Aggregator agg, Fields functionFields, boolean isCombiner) {
        if (isCombiner) {
            if (type == null) {
                type = AggType.FULL_COMBINE;
            }
        } else {
            type = AggType.FULL;
        }
        aggs.add(new AggSpec(inputFields, agg, functionFields));
        return this;
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(CombinerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        initCombiner(inputFields, agg, functionFields);
        return aggregate(functionFields, new CombinerAggregatorCombineImpl(agg), functionFields, true);
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(ReducerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    @Override
    public ChainedFullAggregatorDeclarer aggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return aggregate(inputFields, new ReducerAggregatorImpl(agg), functionFields);
    }

    private void initCombiner(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        stream = stream.each(inputFields, new CombinerAggregatorInitImpl(agg), functionFields);
    }

    private enum AggType {
        PARTITION,
        FULL,
        FULL_COMBINE
    }

    public interface AggregationPartition {
        Stream partition(Stream input);
    }

    // inputFields can be equal to outFields, but multiple aggregators cannot have intersection outFields
    private static class AggSpec {
        Fields inFields;
        Aggregator agg;
        Fields outFields;

        AggSpec(Fields inFields, Aggregator agg, Fields outFields) {
            this.inFields = inFields;
            this.agg = agg;
            this.outFields = outFields;
        }
    }
}
