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
import org.apache.storm.annotation.InterfaceStability;
import org.apache.storm.streams.operations.BiFunction;
import org.apache.storm.streams.operations.CombinerAggregator;
import org.apache.storm.streams.operations.Consumer;
import org.apache.storm.streams.operations.FlatMapFunction;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.operations.IdentityFunction;
import org.apache.storm.streams.operations.PairFlatMapFunction;
import org.apache.storm.streams.operations.PairFunction;
import org.apache.storm.streams.operations.Predicate;
import org.apache.storm.streams.operations.PrintConsumer;
import org.apache.storm.streams.operations.Reducer;
import org.apache.storm.streams.operations.aggregators.Count;
import org.apache.storm.streams.processors.AggregateProcessor;
import org.apache.storm.streams.processors.BranchProcessor;
import org.apache.storm.streams.processors.FilterProcessor;
import org.apache.storm.streams.processors.FlatMapProcessor;
import org.apache.storm.streams.processors.ForEachProcessor;
import org.apache.storm.streams.processors.MapProcessor;
import org.apache.storm.streams.processors.MergeAggregateProcessor;
import org.apache.storm.streams.processors.PeekProcessor;
import org.apache.storm.streams.processors.Processor;
import org.apache.storm.streams.processors.ReduceProcessor;
import org.apache.storm.streams.processors.StateQueryProcessor;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a stream of values.
 *
 * @param <T> the type of the value
 */
@InterfaceStability.Unstable
public class Stream<T> {
    protected static final Fields KEY = new Fields("key");
    protected static final Fields VALUE = new Fields("value");
    protected static final Fields KEY_VALUE = new Fields("key", "value");
    private static final Logger LOG = LoggerFactory.getLogger(Stream.class);
    // the stream builder
    protected final StreamBuilder streamBuilder;
    // the current node
    protected final Node node;
    // the stream id from node's output stream(s) that this stream represents
    protected final String stream;

    Stream(StreamBuilder streamBuilder, Node node) {
        this(streamBuilder, node, node.getOutputStreams().iterator().next());
    }

    private Stream(StreamBuilder streamBuilder, Node node, String stream) {
        this.streamBuilder = streamBuilder;
        this.node = node;
        this.stream = stream;
    }

    /**
     * Returns a stream consisting of the elements of this stream that matches the given filter.
     *
     * @param predicate the predicate to apply to each element to determine if it should be included
     * @return the new stream
     */
    public Stream<T> filter(Predicate<? super T> predicate) {
        return new Stream<>(streamBuilder, addProcessorNode(new FilterProcessor<>(predicate), VALUE, true));
    }

    /**
     * Returns a stream consisting of the result of applying the given mapping function to the values of this stream.
     *
     * @param function a mapping function to be applied to each value in this stream.
     * @return the new stream
     */
    public <R> Stream<R> map(Function<? super T, ? extends R> function) {
        return new Stream<>(streamBuilder, addProcessorNode(new MapProcessor<>(function), VALUE));
    }

    /**
     * Returns a stream of key-value pairs by applying a {@link PairFunction} on each value of this stream.
     *
     * @param function the mapping function to be applied to each value in this stream
     * @param <K>      the key type
     * @param <V>      the value type
     * @return the new stream of key-value pairs
     */
    public <K, V> PairStream<K, V> mapToPair(PairFunction<? super T, ? extends K, ? extends V> function) {
        return new PairStream<>(streamBuilder, addProcessorNode(new MapProcessor<>(function), KEY_VALUE));
    }

    /**
     * Returns a stream consisting of the results of replacing each value of this stream with the contents produced by applying the provided
     * mapping function to each value. This has the effect of applying a one-to-many transformation to the values of the stream, and then
     * flattening the resulting elements into a new stream.
     *
     * @param function a mapping function to be applied to each value in this stream which produces new values.
     * @return the new stream
     */
    public <R> Stream<R> flatMap(FlatMapFunction<? super T, ? extends R> function) {
        return new Stream<>(streamBuilder, addProcessorNode(new FlatMapProcessor<>(function), VALUE));
    }

    /**
     * Returns a stream consisting of the results of replacing each value of this stream with the key-value pairs produced by applying the
     * provided mapping function to each value.
     *
     * @param function the mapping function to be applied to each value in this stream which produces new key-value pairs.
     * @param <K>      the key type
     * @param <V>      the value type
     * @return the new stream of key-value pairs
     *
     * @see #flatMap(FlatMapFunction)
     * @see #mapToPair(PairFunction)
     */
    public <K, V> PairStream<K, V> flatMapToPair(PairFlatMapFunction<? super T, ? extends K, ? extends V> function) {
        return new PairStream<>(streamBuilder, addProcessorNode(new FlatMapProcessor<>(function), KEY_VALUE));
    }

    /**
     * Returns a new stream consisting of the elements that fall within the window as specified by the window parameter. The {@link Window}
     * specification could be used to specify sliding or tumbling windows based on time duration or event count. For example,
     * <pre>
     * // time duration based sliding window
     * stream.window(SlidingWindows.of(Duration.minutes(10), Duration.minutes(1));
     *
     * // count based sliding window
     * stream.window(SlidingWindows.of(Count.(10), Count.of(2)));
     *
     * // time duration based tumbling window
     * stream.window(TumblingWindows.of(Duration.seconds(10));
     * </pre>
     *
     * @see org.apache.storm.streams.windowing.SlidingWindows
     * @see org.apache.storm.streams.windowing.TumblingWindows
     * @param window the window configuration
     * @return the new stream
     */
    public Stream<T> window(Window<?, ?> window) {
        return new Stream<>(streamBuilder, addNode(new WindowNode(window, stream, node.getOutputFields())));
    }

    /**
     * Performs an action for each element of this stream.
     *
     * @param action an action to perform on the elements
     */
    public void forEach(Consumer<? super T> action) {
        addProcessorNode(new ForEachProcessor<>(action), new Fields());
    }

    /**
     * Returns a stream consisting of the elements of this stream, additionally performing the provided action on each element as they are
     * consumed from the resulting stream.
     *
     * @param action the action to perform on the element as they are consumed from the stream
     * @return the new stream
     */
    public Stream<T> peek(Consumer<? super T> action) {
        return new Stream<>(streamBuilder, addProcessorNode(new PeekProcessor<>(action), node.getOutputFields(), true));
    }

    /**
     * Aggregates the values in this stream using the aggregator. This does a global aggregation of values across all partitions.
     * <p>
     * If the stream is windowed, the aggregate result is emitted after each window activation and represents the aggregate of elements that
     * fall within that window. If the stream is not windowed, the aggregate result is emitted as each new element in the stream is
     * processed.
     * </p>
     *
     * @param aggregator the aggregator
     * @param <A>        the accumulator type
     * @param <R>        the result type
     * @return the new stream
     */
    public <A, R> Stream<R> aggregate(CombinerAggregator<? super T, A, ? extends R> aggregator) {
        return combine(aggregator);
    }

    /**
     * Aggregates the values in this stream using the given initial value, accumulator and combiner. This does a global aggregation of
     * values across all partitions.
     * <p>
     * If the stream is windowed, the aggregate result is emitted after each window activation and represents the aggregate of elements that
     * fall within that window. If the stream is not windowed, the aggregate result is emitted as each new element in the stream is
     * processed.
     * </p>
     *
     * @param initialValue the initial value of the result
     * @param accumulator  the accumulator
     * @param combiner     the combiner
     * @param <R>          the result type
     * @return the new stream
     */
    public <R> Stream<R> aggregate(R initialValue,
                                   BiFunction<? super R, ? super T, ? extends R> accumulator,
                                   BiFunction<? super R, ? super R, ? extends R> combiner) {
        return combine(CombinerAggregator.of(initialValue, accumulator, combiner));
    }

    /**
     * Counts the number of values in this stream. This does a global count of values across all partitions.
     * <p>
     * If the stream is windowed, the counts are emitted after each window activation and represents the count of elements that fall within
     * that window. If the stream is not windowed, the count is emitted as each new element in the stream is processed.
     * </p>
     *
     * @return the new stream
     */
    public Stream<Long> count() {
        return aggregate(new Count<>());
    }

    /**
     * Performs a reduction on the elements of this stream, by repeatedly applying the reducer. This does a global reduction of values
     * across all partitions.
     * <p>
     * If the stream is windowed, the result is emitted after each window activation and represents the reduction of elements that fall
     * within that window. If the stream is not windowed, the result is emitted as each new element in the stream is processed.
     * </p>
     *
     * @param reducer the reducer
     * @return the new stream
     */
    public Stream<T> reduce(Reducer<T> reducer) {
        return combine(reducer);
    }

    /**
     * Returns a new stream with the given value of parallelism. Further operations on this stream would execute at this level of
     * parallelism.
     *
     * @param parallelism the parallelism value
     * @return the new stream
     */
    public Stream<T> repartition(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("Parallelism should be >= 1");
        }
        if (node.getParallelism() == parallelism) {
            LOG.debug("Node's current parallelism {}, new parallelism {}", node.getParallelism(), parallelism);
            return this;
        }
        Node partitionNode = addNode(node, new PartitionNode(stream, node.getOutputFields()), parallelism);
        return new Stream<>(streamBuilder, partitionNode);
    }

    /**
     * Returns an array of streams by splitting the given stream into multiple branches based on the given predicates. The predicates are
     * applied in the given order to the values of this stream and the result is forwarded to the corresponding (index based) result stream
     * based on the (index of) predicate that matches.
     * <p>
     * <b>Note:</b> If none of the predicates match a value, that value is dropped.
     * </p>
     *
     * @param predicates the predicates
     * @return an array of result streams (branches) corresponding to the given predicates
     */
    @SuppressWarnings("unchecked")
    public Stream<T>[] branch(Predicate<? super T>... predicates) {
        List<Stream<T>> childStreams = new ArrayList<>();
        if (predicates.length > 0) {
            BranchProcessor<T> branchProcessor = new BranchProcessor<>();
            Node branchNode = addProcessorNode(branchProcessor, VALUE);
            for (Predicate<? super T> predicate : predicates) {
                // create a child node (identity) per branch
                ProcessorNode child = makeProcessorNode(new MapProcessor<>(new IdentityFunction<>()), node.getOutputFields());
                String branchStream = child.getOutputStreams().iterator().next() + "-branch";
                // branchStream is the parent stream that connects branch processor to this child
                branchNode.addOutputStream(branchStream);
                addNode(branchNode, child, branchStream);
                childStreams.add(new Stream<>(streamBuilder, child));
                branchProcessor.addPredicate(predicate, branchStream);
            }
        }
        return childStreams.toArray((Stream<T>[]) new Stream[childStreams.size()]);
    }

    /**
     * Print the values in this stream.
     */
    public void print() {
        forEach(new PrintConsumer<T>());
    }

    /**
     * Sends the elements of this stream to a bolt. This could be used to plug in existing bolts as sinks in the stream, for e.g. a {@code
     * RedisStoreBolt}. The bolt would have a parallelism of 1.
     * <p>
     * <b>Note:</b> This would provide guarantees only based on what the bolt provides.
     * </p>
     *
     * @param bolt the bolt
     */
    public void to(IRichBolt bolt) {
        to(bolt, 1);
    }

    /**
     * Sends the elements of this stream to a bolt. This could be used to plug in existing bolts as sinks in the stream, for e.g. a {@code
     * RedisStoreBolt}.
     * <p>
     * <b>Note:</b> This would provide guarantees only based on what the bolt provides.
     * </p>
     *
     * @param bolt        the bolt
     * @param parallelism the parallelism of the bolt
     */
    public void to(IRichBolt bolt, int parallelism) {
        addSinkNode(new SinkNode(bolt), parallelism);
    }

    /**
     * Sends the elements of this stream to a bolt. This could be used to plug in existing bolts as sinks in the stream, for e.g. a {@code
     * RedisStoreBolt}. The bolt would have a parallelism of 1.
     * <p>
     * <b>Note:</b> This would provide guarantees only based on what the bolt provides.
     * </p>
     *
     * @param bolt the bolt
     */
    public void to(IBasicBolt bolt) {
        to(bolt, 1);
    }

    /**
     * Sends the elements of this stream to a bolt. This could be used to plug in existing bolts as sinks in the stream, for e.g. a {@code
     * RedisStoreBolt}.
     * <p>
     * <b>Note:</b> This would provide guarantees only based on what the bolt provides.
     * </p>
     *
     * @param bolt        the bolt
     * @param parallelism the parallelism of the bolt
     */
    public void to(IBasicBolt bolt, int parallelism) {
        addSinkNode(new SinkNode(bolt), parallelism);
    }

    /**
     * Queries the given stream state with the values in this stream as the keys.
     *
     * @param streamState the stream state
     * @param <V>         the value type
     * @return the result stream
     */
    public <V> PairStream<T, V> stateQuery(StreamState<T, V> streamState) {
        // need field grouping for state query so that the query is routed to the correct task
        Node newNode = partitionBy(VALUE, node.getParallelism()).addProcessorNode(new StateQueryProcessor<>(streamState), KEY_VALUE);
        return new PairStream<>(streamBuilder, newNode);
    }

    Node getNode() {
        return node;
    }

    Node addNode(Node parent, Node child, int parallelism) {
        return streamBuilder.addNode(parent, child, parallelism);
    }

    Node addNode(Node child) {
        return addNode(node, child);
    }

    private Node addNode(Node parent, Node child) {
        return streamBuilder.addNode(parent, child);
    }

    private Node addNode(Node parent, Node child, String parentStreamId) {
        return streamBuilder.addNode(parent, child, parentStreamId);
    }

    private Node addNode(Node parent, Node child, String parentStreamId, int parallelism) {
        return streamBuilder.addNode(parent, child, parentStreamId, parallelism);
    }

    Node addProcessorNode(Processor<?> processor, Fields outputFields) {
        return addNode(makeProcessorNode(processor, outputFields));
    }

    Node addProcessorNode(Processor<?> processor, Fields outputFields, boolean preservesKey) {
        return addNode(makeProcessorNode(processor, outputFields, preservesKey));
    }

    String getStream() {
        return stream;
    }

    private ProcessorNode makeProcessorNode(Processor<?> processor, Fields outputFields) {
        return makeProcessorNode(processor, outputFields, false);
    }

    ProcessorNode makeProcessorNode(Processor<?> processor, Fields outputFields, boolean preservesKey) {
        return new ProcessorNode(processor, UniqueIdGen.getInstance().getUniqueStreamId(), outputFields, preservesKey);
    }

    private void addSinkNode(SinkNode sinkNode, int parallelism) {
        String boltId = UniqueIdGen.getInstance().getUniqueBoltId();
        sinkNode.setComponentId(boltId);
        sinkNode.setParallelism(parallelism);
        if (node instanceof SpoutNode) {
            addNode(node, sinkNode, Utils.DEFAULT_STREAM_ID, parallelism);
        } else {
            addNode(node, sinkNode, parallelism);
        }
    }

    private Stream<T> global() {
        Node partitionNode = addNode(new PartitionNode(stream, node.getOutputFields(), GroupingInfo.global()));
        return new Stream<>(streamBuilder, partitionNode);
    }

    protected Stream<T> partitionBy(Fields fields, int parallelism) {
        return new Stream<>(
            streamBuilder,
            addNode(node, new PartitionNode(stream, node.getOutputFields(), GroupingInfo.fields(fields)), parallelism));
    }

    private boolean shouldPartition() {
        return node.getParallelism() > 1;
    }

    private <A> Stream<A> combinePartition(CombinerAggregator<? super T, A, ?> aggregator) {
        return new Stream<>(streamBuilder,
                            addProcessorNode(new AggregateProcessor<>(aggregator, true), VALUE, true));
    }

    private <R> Stream<R> merge(CombinerAggregator<?, T, ? extends R> aggregator) {
        return new Stream<>(streamBuilder,
                            addProcessorNode(new MergeAggregateProcessor<>(aggregator), VALUE));
    }

    private <A, R> Stream<R> aggregatePartition(CombinerAggregator<? super T, A, ? extends R> aggregator) {
        return new Stream<>(streamBuilder, addProcessorNode(new AggregateProcessor<>(aggregator), VALUE));
    }

    private Stream<T> reducePartition(Reducer<T> reducer) {
        return new Stream<>(streamBuilder, addProcessorNode(new ReduceProcessor<>(reducer), VALUE));
    }

    // if re-partitioning is involved, does a per-partition aggregate before emitting the results downstream
    private <A, R> Stream<R> combine(CombinerAggregator<? super T, A, ? extends R> aggregator) {
        if (shouldPartition()) {
            if (node instanceof ProcessorNode) {
                if (node.isWindowed()) {
                    return combinePartition(aggregator).global().merge(aggregator);
                }
            } else if (node instanceof WindowNode) {
                Set<Node> parents = node.getParents();
                Optional<Node> nonWindowed = parents.stream().filter(p -> !p.isWindowed()).findAny();
                if (!nonWindowed.isPresent()) {
                    parents.forEach(p -> {
                        Node localAggregateNode = makeProcessorNode(
                            new AggregateProcessor<>(aggregator, true), VALUE, true);
                        streamBuilder.insert(p, localAggregateNode);
                    });
                    return ((Stream<A>) global()).merge(aggregator);
                }
            }
            return global().aggregatePartition(aggregator);
        } else {
            return aggregatePartition(aggregator);
        }
    }

    // if re-partitioning is involved, does a per-partition reduce before emitting the results downstream
    private Stream<T> combine(Reducer<T> reducer) {
        if (shouldPartition()) {
            if (node instanceof ProcessorNode) {
                if (node.isWindowed()) {
                    return reducePartition(reducer).global().reducePartition(reducer);
                }
            } else if (node instanceof WindowNode) {
                for (Node p : node.getParents()) {
                    if (p.isWindowed()) {
                        Node localReduceNode = makeProcessorNode(new ReduceProcessor<>(reducer), VALUE);
                        streamBuilder.insert(p, localReduceNode);
                    }
                }
            }
            return global().reducePartition(reducer);
        } else {
            return reducePartition(reducer);
        }
    }

}
