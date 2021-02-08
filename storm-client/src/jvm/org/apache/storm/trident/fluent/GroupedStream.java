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

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.impl.GroupedAggregator;
import org.apache.storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;
import org.apache.storm.trident.state.QueryFunction;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateSpec;
import org.apache.storm.trident.state.map.MapCombinerAggStateUpdater;
import org.apache.storm.trident.state.map.MapReducerAggStateUpdater;
import org.apache.storm.trident.util.TridentUtils;
import org.apache.storm.tuple.Fields;


public class GroupedStream implements IAggregatableStream, GlobalAggregationScheme<GroupedStream> {
    Fields groupFields;
    Stream stream;

    public GroupedStream(Stream stream, Fields groupFields) {
        this.groupFields = groupFields;
        this.stream = stream;
    }

    public GroupedStream name(String name) {
        return new GroupedStream(stream.name(name), groupFields);
    }

    public ChainedAggregatorDeclarer chainedAgg() {
        return new ChainedAggregatorDeclarer(this, this);
    }

    public Stream aggregate(Aggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public Stream aggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        return new ChainedAggregatorDeclarer(this, this)
            .aggregate(inputFields, agg, functionFields)
            .chainEnd();
    }

    public Stream aggregate(CombinerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public Stream aggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        return new ChainedAggregatorDeclarer(this, this)
            .aggregate(inputFields, agg, functionFields)
            .chainEnd();
    }

    public Stream aggregate(ReducerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public Stream aggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return new ChainedAggregatorDeclarer(this, this)
            .aggregate(inputFields, agg, functionFields)
            .chainEnd();
    }

    public TridentState persistentAggregate(StateFactory stateFactory, CombinerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, CombinerAggregator agg, Fields functionFields) {
        return persistentAggregate(spec, null, agg, functionFields);
    }

    public TridentState persistentAggregate(StateFactory stateFactory, Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), inputFields, agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        return aggregate(inputFields, agg, functionFields)
            .partitionPersist(spec,
                              TridentUtils.fieldsUnion(groupFields, functionFields),
                              new MapCombinerAggStateUpdater(agg, groupFields, functionFields),
                              TridentUtils.fieldsConcat(groupFields, functionFields));
    }

    public TridentState persistentAggregate(StateFactory stateFactory, Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), inputFields, agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return stream.partitionBy(groupFields)
                      .partitionPersist(spec,
                                        TridentUtils.fieldsUnion(groupFields, inputFields),
                                        new MapReducerAggStateUpdater(agg, groupFields, inputFields),
                                        TridentUtils.fieldsConcat(groupFields, functionFields));
    }

    public TridentState persistentAggregate(StateFactory stateFactory, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(spec, null, agg, functionFields);
    }

    public Stream stateQuery(TridentState state, Fields inputFields, QueryFunction function, Fields functionFields) {
        return stream.partitionBy(groupFields)
                      .stateQuery(state,
                                  inputFields,
                                  function,
                                  functionFields);
    }

    public Stream stateQuery(TridentState state, QueryFunction function, Fields functionFields) {
        return stateQuery(state, null, function, functionFields);
    }

    @Override
    public IAggregatableStream each(Fields inputFields, Function function, Fields functionFields) {
        Stream s = stream.each(inputFields, function, functionFields);
        return new GroupedStream(s, groupFields);
    }

    @Override
    public IAggregatableStream partitionAggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        Aggregator groupedAgg = new GroupedAggregator(agg, groupFields, inputFields, functionFields.size());
        Fields allInFields = TridentUtils.fieldsUnion(groupFields, inputFields);
        Fields allOutFields = TridentUtils.fieldsConcat(groupFields, functionFields);
        Stream s = stream.partitionAggregate(allInFields, groupedAgg, allOutFields);
        return new GroupedStream(s, groupFields);
    }

    @Override
    public IAggregatableStream aggPartition(GroupedStream s) {
        return new GroupedStream(s.stream.partitionBy(groupFields), groupFields);
    }

    @Override
    public Stream toStream() {
        return stream;
    }

    @Override
    public Fields getOutputFields() {
        return stream.getOutputFields();
    }

    public Fields getGroupFields() {
        return groupFields;
    }

    @Override
    public BatchToPartition singleEmitPartitioner() {
        return null;
    }
}
