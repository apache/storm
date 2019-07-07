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

package org.apache.storm.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.annotation.InterfaceStability;
import org.apache.storm.streams.operations.BiFunction;
import org.apache.storm.streams.operations.CombinerAggregator;
import org.apache.storm.streams.operations.Consumer;
import org.apache.storm.streams.operations.FlatMapFunction;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.operations.PairValueJoiner;
import org.apache.storm.streams.operations.Predicate;
import org.apache.storm.streams.operations.Reducer;
import org.apache.storm.streams.operations.StateUpdater;
import org.apache.storm.streams.operations.ValueJoiner;
import org.apache.storm.streams.operations.aggregators.Count;
import org.apache.storm.streams.processors.AggregateByKeyProcessor;
import org.apache.storm.streams.processors.CoGroupByKeyProcessor;
import org.apache.storm.streams.processors.FlatMapValuesProcessor;
import org.apache.storm.streams.processors.JoinProcessor;
import org.apache.storm.streams.processors.MapValuesProcessor;
import org.apache.storm.streams.processors.MergeAggregateByKeyProcessor;
import org.apache.storm.streams.processors.ReduceByKeyProcessor;
import org.apache.storm.streams.processors.UpdateStateByKeyProcessor;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.tuple.Fields;


/**
 * Represents a stream of key-value pairs.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@InterfaceStability.Unstable
public class PairStream<K, V> extends Stream<Pair<K, V>> {

    PairStream(StreamBuilder topology, Node node) {
        super(topology, node);
        node.setEmitsPair(true);
    }

    /**
     * Returns a new stream by applying a {@link Function} to the value of each key-value pairs in this stream.
     *
     * @param function the mapping function
     * @param <R>      the result type
     * @return the new stream
     */
    public <R> PairStream<K, R> mapValues(Function<? super V, ? extends R> function) {
        return new PairStream<>(streamBuilder,
                                addProcessorNode(new MapValuesProcessor<>(function), KEY_VALUE, true));
    }

    /**
     * Return a new stream by applying a {@link FlatMapFunction} function to the value of each key-value pairs in this stream.
     *
     * @param function the flatmap function
     * @param <R>      the result type
     * @return the new stream
     */
    public <R> PairStream<K, R> flatMapValues(FlatMapFunction<? super V, ? extends R> function) {
        return new PairStream<>(streamBuilder,
                                addProcessorNode(new FlatMapValuesProcessor<>(function), KEY_VALUE, true));
    }

    /**
     * Aggregates the values for each key of this stream using the given initial value, accumulator and combiner.
     *
     * @param initialValue the initial value of the result
     * @param accumulator  the accumulator
     * @param combiner     the combiner
     * @param <R>          the result type
     * @return the new stream
     */
    public <R> PairStream<K, R> aggregateByKey(R initialValue,
                                               BiFunction<? super R, ? super V, ? extends R> accumulator,
                                               BiFunction<? super R, ? super R, ? extends R> combiner) {
        return combineByKey(CombinerAggregator.of(initialValue, accumulator, combiner));
    }

    /**
     * Aggregates the values for each key of this stream using the given {@link CombinerAggregator}.
     *
     * @param aggregator the combiner aggregator
     * @param <A>        the accumulator type
     * @param <R>        the result type
     * @return the new stream
     */
    public <A, R> PairStream<K, R> aggregateByKey(CombinerAggregator<? super V, A, ? extends R> aggregator) {
        return combineByKey(aggregator);
    }

    /**
     * Counts the values for each key of this stream.
     *
     * @return the new stream
     */
    public PairStream<K, Long> countByKey() {
        return aggregateByKey(new Count<>());
    }

    /**
     * Performs a reduction on the values for each key of this stream by repeatedly applying the reducer.
     *
     * @param reducer the reducer
     * @return the new stream
     */
    public PairStream<K, V> reduceByKey(Reducer<V> reducer) {
        return combineByKey(reducer);
    }

    /**
     * Returns a new stream where the values are grouped by the keys.
     *
     * @return the new stream
     */
    public PairStream<K, Iterable<V>> groupByKey() {
        return partitionByKey().aggregatePartition(new MergeValues<>());
    }

    /**
     * Returns a new stream where the values are grouped by keys and the given window. The values that arrive within a window having the
     * same key will be merged together and returned as an Iterable of values mapped to the key.
     *
     * @param window the window configuration
     * @return the new stream
     */
    public PairStream<K, Iterable<V>> groupByKeyAndWindow(Window<?, ?> window) {
        return partitionByKey().window(window).aggregatePartition(new MergeValues<>());
    }

    /**
     * Returns a new stream where the values that arrive within a window having the same key will be reduced by repeatedly applying the
     * reducer.
     *
     * @param reducer the reducer
     * @param window  the window configuration
     * @return the new stream
     */
    public PairStream<K, V> reduceByKeyAndWindow(Reducer<V> reducer, Window<?, ?> window) {
        return partitionByKey().window(window).reduceByKey(reducer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PairStream<K, V> peek(Consumer<? super Pair<K, V>> action) {
        return toPairStream(super.peek(action));
    }

    /**
     * {@inheritDoc}
     */
    public PairStream<K, V> filter(Predicate<? super Pair<K, V>> predicate) {
        return toPairStream(super.filter(predicate));
    }

    /**
     * Join the values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism of this stream is carried forward to the joined stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <V1> PairStream<K, Pair<V, V1>> join(PairStream<K, V1> otherStream) {
        return join(otherStream, new PairValueJoiner<>());
    }

    /**
     * Join the values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism of this stream is carried forward to the joined stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param valueJoiner the {@link ValueJoiner}
     * @param <R>         the type of the values resulting from the join
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <R, V1> PairStream<K, R> join(PairStream<K, V1> otherStream,
            ValueJoiner<? super V, ? super V1, ? extends R> valueJoiner) {
        return partitionByKey()
                .joinPartition(
                        otherStream.partitionByKey(),
                        valueJoiner,
                        JoinProcessor.JoinType.INNER,
                        JoinProcessor.JoinType.INNER);
    }

    /**
     * Does a left outer join of the values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism of this stream is carried forward to the joined stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <V1> PairStream<K, Pair<V, V1>> leftOuterJoin(PairStream<K, V1> otherStream) {
        return leftOuterJoin(otherStream, new PairValueJoiner<>());
    }

    /**
     * Does a left outer join of the values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism of this stream is carried forward to the joined stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param valueJoiner the {@link ValueJoiner}
     * @param <R>         the type of the values resulting from the join
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <R, V1> PairStream<K, R> leftOuterJoin(PairStream<K, V1> otherStream,
            ValueJoiner<? super V, ? super V1, ? extends R> valueJoiner) {
        return partitionByKey()
                .joinPartition(
                        otherStream.partitionByKey(),
                        valueJoiner,
                        JoinProcessor.JoinType.OUTER,
                        JoinProcessor.JoinType.INNER);
    }

    /**
     * Does a right outer join of the values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism of this stream is carried forward to the joined stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <V1> PairStream<K, Pair<V, V1>> rightOuterJoin(PairStream<K, V1> otherStream) {
        return rightOuterJoin(otherStream, new PairValueJoiner<>());
    }

    /**
     * Does a right outer join of the values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism of this stream is carried forward to the joined stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param valueJoiner the {@link ValueJoiner}
     * @param <R>         the type of the values resulting from the join
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <R, V1> PairStream<K, R> rightOuterJoin(PairStream<K, V1> otherStream,
            ValueJoiner<? super V, ? super V1, ? extends R> valueJoiner) {
        return partitionByKey()
                .joinPartition(
                        otherStream.partitionByKey(),
                        valueJoiner,
                        JoinProcessor.JoinType.INNER,
                        JoinProcessor.JoinType.OUTER);
    }

    /**
     * Does a full outer join of the values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism of this stream is carried forward to the joined stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <V1> PairStream<K, Pair<V, V1>> fullOuterJoin(PairStream<K, V1> otherStream) {
        return fullOuterJoin(otherStream, new PairValueJoiner<>());
    }

    /**
     * Does a full outer join of the values of this stream with the values having the same key from the other stream.
     * <p>
     * Note: The parallelism of this stream is carried forward to the joined stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param valueJoiner the {@link ValueJoiner}
     * @param <R>         the type of the values resulting from the join
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <R, V1> PairStream<K, R> fullOuterJoin(PairStream<K, V1> otherStream,
            ValueJoiner<? super V, ? super V1, ? extends R> valueJoiner) {
        return partitionByKey()
                .joinPartition(
                        otherStream.partitionByKey(),
                        valueJoiner,
                        JoinProcessor.JoinType.OUTER,
                        JoinProcessor.JoinType.OUTER);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PairStream<K, V> window(Window<?, ?> window) {
        return toPairStream(super.window(window));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PairStream<K, V> repartition(int parallelism) {
        return toPairStream(super.repartition(parallelism));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public PairStream<K, V>[] branch(Predicate<? super Pair<K, V>>... predicates) {
        List<PairStream<K, V>> pairStreams = new ArrayList<>();
        for (Stream<Pair<K, V>> stream : super.branch(predicates)) {
            pairStreams.add(toPairStream(stream));
        }
        return pairStreams.toArray(new PairStream[pairStreams.size()]);
    }

    /**
     * Update the state by applying the given state update function to the previous state of the key and the new value for the key. This
     * internally uses {@link org.apache.storm.topology.IStatefulBolt} to save the state. Use {@link Config#TOPOLOGY_STATE_PROVIDER} to
     * choose the state implementation.
     *
     * @param stateUpdateFn the state update function
     * @param <R>           the result type
     * @return the {@link StreamState} which can be used to query the state
     */
    public <R> StreamState<K, R> updateStateByKey(R initialValue,
                                                  BiFunction<? super R, ? super V, ? extends R> stateUpdateFn) {
        return updateStateByKey(StateUpdater.of(initialValue, stateUpdateFn));
    }

    /**
     * Update the state by applying the given state update function to the previous state of the key and the new value for the key. This
     * internally uses {@link org.apache.storm.topology.IStatefulBolt} to save the state. Use {@link Config#TOPOLOGY_STATE_PROVIDER} to
     * choose the state implementation.
     *
     * @param stateUpdater the state updater
     * @param <R>          the result type
     * @return the {@link StreamState} which can be used to query the state
     */
    public <R> StreamState<K, R> updateStateByKey(StateUpdater<? super V, ? extends R> stateUpdater) {
        // repartition so that state query fields grouping works correctly. this can be optimized further
        return partitionBy(KEY).updateStateByKeyPartition(stateUpdater);
    }

    /**
     * Groups the values of this stream with the values having the same key from the other stream.
     * <p>
     * If stream1 has values - (k1, v1), (k2, v2), (k2, v3) <br/> and stream2 has values - (k1, x1), (k1, x2), (k3, x3) <br/> The the
     * co-grouped stream would contain - (k1, ([v1], [x1, x2]), (k2, ([v2, v3], [])), (k3, ([], [x3]))
     * </p>
     * <p>
     * Note: The parallelism of this stream is carried forward to the co-grouped stream.
     * </p>
     *
     * @param otherStream the other stream
     * @param <V1>        the type of the values in the other stream
     * @return the new stream
     */
    public <V1> PairStream<K, Pair<Iterable<V>, Iterable<V1>>> coGroupByKey(PairStream<K, V1> otherStream) {
        return partitionByKey().coGroupByKeyPartition(otherStream);
    }


    private <R> StreamState<K, R> updateStateByKeyPartition(StateUpdater<? super V, ? extends R> stateUpdater) {
        return new StreamState<>(
            new PairStream<>(streamBuilder,
                             addProcessorNode(new UpdateStateByKeyProcessor<>(stateUpdater), KEY_VALUE, true)));
    }

    private <R, V1> PairStream<K, R> joinPartition(PairStream<K, V1> otherStream,
                                                   ValueJoiner<? super V, ? super V1, ? extends R> valueJoiner,
                                                   JoinProcessor.JoinType leftType,
                                                   JoinProcessor.JoinType rightType) {
        String leftStream = stream;
        String rightStream = otherStream.stream;
        Node joinNode = addProcessorNode(
            new JoinProcessor<>(leftStream, rightStream, valueJoiner, leftType, rightType),
            KEY_VALUE,
            true);
        addNode(otherStream.getNode(), joinNode, joinNode.getParallelism());
        return new PairStream<>(streamBuilder, joinNode);
    }

    private <R, V1> PairStream<K, R> coGroupByKeyPartition(PairStream<K, V1> otherStream) {
        String firstStream = stream;
        String secondStream = otherStream.stream;
        Node coGroupNode = addProcessorNode(
            new CoGroupByKeyProcessor<>(firstStream, secondStream),
            KEY_VALUE,
            true);
        addNode(otherStream.getNode(), coGroupNode, coGroupNode.getParallelism());
        return new PairStream<>(streamBuilder, coGroupNode);
    }

    private PairStream<K, V> partitionByKey() {
        return shouldPartitionByKey() ? partitionBy(KEY) : this;
    }

    private boolean shouldPartitionByKey() {
        if (node.getParallelism() == 1) {
            return false;
        }
        /*
         * if the current processor preserves the key and is
         * already partitioned on key, skip the re-partition.
         */
        if (node instanceof ProcessorNode) {
            ProcessorNode pn = (ProcessorNode) node;
            Fields fields = pn.getGroupingInfo() == null ? null : pn.getGroupingInfo().getFields();
            if (pn.isPreservesKey() && fields != null && fields.equals(KEY)) {
                return false;
            }
        }
        return true;
    }

    private PairStream<K, V> partitionBy(Fields fields) {
        return toPairStream(partitionBy(fields, node.parallelism));
    }

    private PairStream<K, V> toPairStream(Stream<Pair<K, V>> stream) {
        return new PairStream<>(stream.streamBuilder, stream.node);
    }

    private <A, R> PairStream<K, R> aggregatePartition(CombinerAggregator<? super V, A, ? extends R> aggregator) {
        return new PairStream<>(streamBuilder,
                                addProcessorNode(new AggregateByKeyProcessor<>(aggregator), KEY_VALUE, true));
    }

    private <A> PairStream<K, A> combinePartition(CombinerAggregator<? super V, A, ?> aggregator) {
        return new PairStream<>(streamBuilder,
                                addProcessorNode(new AggregateByKeyProcessor<>(aggregator, true), KEY_VALUE, true));
    }

    private <R> PairStream<K, R> merge(CombinerAggregator<?, V, ? extends R> aggregator) {
        return new PairStream<>(streamBuilder,
                                addProcessorNode(new MergeAggregateByKeyProcessor<>(aggregator), KEY_VALUE, true));
    }

    private PairStream<K, V> reducePartition(Reducer<V> reducer) {
        return new PairStream<>(streamBuilder,
                                addProcessorNode(new ReduceByKeyProcessor<>(reducer), KEY_VALUE, true));
    }

    // if re-partitioning is involved, does a per-partition aggregate by key before emitting the results downstream
    private <A, R> PairStream<K, R> combineByKey(CombinerAggregator<? super V, A, ? extends R> aggregator) {
        if (shouldPartitionByKey()) {
            if (node instanceof ProcessorNode) {
                if (node.isWindowed()) {
                    return combinePartition(aggregator).partitionBy(KEY).merge(aggregator);
                }
            } else if (node instanceof WindowNode) {
                Set<Node> parents = node.getParents();
                Optional<Node> nonWindowed = parents.stream().filter(p -> !p.isWindowed()).findAny();
                if (!nonWindowed.isPresent()) {
                    parents.forEach(p -> {
                        Node localAggregateNode = makeProcessorNode(
                            new AggregateByKeyProcessor<>(aggregator, true), KEY_VALUE, true);
                        streamBuilder.insert(p, localAggregateNode);
                    });
                    return ((PairStream<K, A>) partitionBy(KEY)).merge(aggregator);
                }
            }
            return partitionBy(KEY).aggregatePartition(aggregator);
        } else {
            return aggregatePartition(aggregator);
        }
    }

    // if re-partitioning is involved, does a per-partition reduce by key before emitting the results downstream
    private PairStream<K, V> combineByKey(Reducer<V> reducer) {
        if (shouldPartitionByKey()) {
            if (node instanceof ProcessorNode) {
                if (node.isWindowed()) {
                    return reducePartition(reducer).partitionBy(KEY).reducePartition(reducer);
                }
            } else if (node instanceof WindowNode) {
                for (Node p : node.getParents()) {
                    if (p.isWindowed()) {
                        Node localReduceNode = makeProcessorNode(new ReduceByKeyProcessor<>(reducer), KEY_VALUE, true);
                        streamBuilder.insert(p, localReduceNode);
                    }
                }
            }
            return partitionBy(KEY).reducePartition(reducer);
        } else {
            return reducePartition(reducer);
        }
    }

    // used internally to merge values in groupByKeyAndWindow
    private static class MergeValues<V> implements CombinerAggregator<V, ArrayList<V>, ArrayList<V>> {
        @Override
        public ArrayList<V> init() {
            return new ArrayList<>();
        }

        @Override
        public ArrayList<V> apply(ArrayList<V> aggregate, V value) {
            aggregate.add(value);
            return aggregate;
        }

        @Override
        public ArrayList<V> merge(ArrayList<V> accum1, ArrayList<V> accum2) {
            ArrayList<V> res = new ArrayList<V>();
            res.addAll(accum1);
            res.addAll(accum2);
            return res;
        }

        @Override
        public ArrayList<V> result(ArrayList<V> accum) {
            return accum;
        }
    }
}
