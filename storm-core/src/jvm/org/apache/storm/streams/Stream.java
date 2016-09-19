/**
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
 */
package org.apache.storm.streams;

import org.apache.storm.streams.operations.Aggregator;
import org.apache.storm.streams.operations.Consumer;
import org.apache.storm.streams.operations.FlatMapFunction;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.operations.IdentityFunction;
import org.apache.storm.streams.operations.PairFlatMapFunction;
import org.apache.storm.streams.operations.PairFunction;
import org.apache.storm.streams.operations.Predicate;
import org.apache.storm.streams.operations.PrintConsumer;
import org.apache.storm.streams.operations.Reducer;
import org.apache.storm.streams.processors.AggregateProcessor;
import org.apache.storm.streams.processors.BranchProcessor;
import org.apache.storm.streams.processors.FilterProcessor;
import org.apache.storm.streams.processors.FlatMapProcessor;
import org.apache.storm.streams.processors.ForEachProcessor;
import org.apache.storm.streams.processors.MapProcessor;
import org.apache.storm.streams.processors.PeekProcessor;
import org.apache.storm.streams.processors.Processor;
import org.apache.storm.streams.processors.ReduceProcessor;
import org.apache.storm.streams.processors.StateQueryProcessor;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a stream of values.
 *
 * @param <T> the type of the value
 */
public class Stream<T> {
    protected static final Fields KEY = new Fields("key");
    protected static final Fields VALUE = new Fields("value");
    protected static final Fields KEY_VALUE = new Fields("key", "value");
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
        return new Stream<>(streamBuilder, addProcessorNode(new FilterProcessor<>(predicate), VALUE));
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
    public <K, V> PairStream<K, V> mapToPair(PairFunction<T, K, V> function) {
        return new PairStream<>(streamBuilder, addProcessorNode(new MapProcessor<>(function), KEY_VALUE));
    }

    /**
     * Returns a stream consisting of the results of replacing each value of this stream with the contents
     * produced by applying the provided mapping function to each value. This has the effect of applying
     * a one-to-many transformation to the values of the stream, and then flattening the resulting elements
     * into a new stream.
     *
     * @param function a mapping function to be applied to each value in this stream which produces new values.
     * @return the new stream
     */
    public <R> Stream<R> flatMap(FlatMapFunction<T, R> function) {
        return new Stream<>(streamBuilder, addProcessorNode(new FlatMapProcessor<>(function), VALUE));
    }

    /**
     * Returns a stream consisting of the results of replacing each value of this stream with the key-value pairs
     * produced by applying the provided mapping function to each value.
     *
     * @param function the mapping function to be applied to each value in this stream which produces new key-value pairs.
     * @param <K>      the key type
     * @param <V>      the value type
     * @return the new stream of key-value pairs
     * @see #flatMap(FlatMapFunction)
     * @see #mapToPair(PairFunction)
     */
    public <K, V> PairStream<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
        return new PairStream<>(streamBuilder, addProcessorNode(new FlatMapProcessor<>(function), KEY_VALUE));
    }

    /**
     * Returns a new stream consisting of the elements that fall within the window as specified by the window parameter.
     * The {@link Window} specification could be used to specify sliding or tumbling windows based on
     * time duration or event count. For example,
     * <pre>
     * // time duration based sliding window
     * stream.window(SlidingWindows.of(Duration.minutes(10), Duration.minutes(1));
     *
     * // count based sliding window
     * stream.window(SlidingWindows.of(Count.(10), Count.of(2)));
     *
     * // time duration based tumbling window
     * stream.window(TumblingWindows.of(Duration.seconds(10));
     * </p>
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
     * Returns a stream consisting of the elements of this stream, additionally performing the provided action on
     * each element as they are consumed from the resulting stream.
     *
     * @param action the action to perform on the element as they are consumed from the stream
     * @return the new stream
     */
    public Stream<T> peek(Consumer<? super T> action) {
        return new Stream<>(streamBuilder, addProcessorNode(new PeekProcessor<>(action), node.getOutputFields()));
    }

    /**
     * Aggregates the values in this stream using the aggregator. This does a global aggregation, i.e. the elements
     * across all the partitions are forwarded to a single task for computing the aggregate.
     * <p>
     * If the stream is windowed, the aggregate result is emitted after each window activation and represents the
     * aggregate of elements that fall within that window.
     * If the stream is not windowed, the aggregate result is emitted as each new element in the stream is processed.
     * </p>
     *
     * @param aggregator the aggregator
     * @param <R>        the result type
     * @return the new stream
     */
    public <R> Stream<R> aggregate(Aggregator<? super T, ? extends R> aggregator) {
        return new Stream<>(streamBuilder, global().addProcessorNode(new AggregateProcessor<>(aggregator), VALUE));
    }

    /**
     * Performs a reduction on the elements of this stream, by repeatedly applying the reducer.
     * <p>
     * If the stream is windowed, the result is emitted after each window activation and represents the
     * reduction of elements that fall within that window.
     * If the stream is not windowed, the result is emitted as each new element in the stream is processed.
     * </p>
     *
     * @param reducer the reducer
     * @return the new stream
     */
    public Stream<T> reduce(Reducer<T> reducer) {
        return new Stream<>(streamBuilder, global().addProcessorNode(new ReduceProcessor<>(reducer), VALUE));
    }

    /**
     * Returns a new stream with the given value of parallelism. Further operations on this stream
     * would execute at this level of parallelism.
     *
     * @param parallelism the parallelism value
     * @return the new stream
     */
    public Stream<T> repartition(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("Parallelism should be >= 1");
        }
        Node partitionNode = addNode(node, new PartitionNode(stream, node.getOutputFields()), parallelism);
        return new Stream<>(streamBuilder, partitionNode);
    }

    /**
     * Returns an array of streams by splitting the given stream into multiple branches based on the given
     * predicates. The predicates are applied in the given order to the values of this stream and the result
     * is forwarded to the corresponding (index based) result stream based on the (index of) predicate that matches.
     * If none of the predicates match a value, that value is dropped.
     *
     * @param predicates the predicates
     * @return an array of result streams (branches) corresponding to the given predicates
     */
    @SuppressWarnings("unchecked")
    public Stream<T>[] branch(Predicate<T>... predicates) {
        List<Stream<T>> childStreams = new ArrayList<>();
        if (predicates.length > 0) {
            BranchProcessor<T> branchProcessor = new BranchProcessor<>();
            Node branchNode = addProcessorNode(branchProcessor, VALUE);
            for (Predicate<T> predicate : predicates) {
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
     * Sends the elements of this stream to a bolt. This could be used to plug in existing bolts as
     * sinks in the stream, for e.g. a {@code RedisStoreBolt}. The bolt would have a parallelism of 1.
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
     * Sends the elements of this stream to a bolt. This could be used to plug in existing bolts as
     * sinks in the stream, for e.g. a {@code RedisStoreBolt}.
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
     * Sends the elements of this stream to a bolt. This could be used to plug in existing bolts as
     * sinks in the stream, for e.g. a {@code RedisStoreBolt}. The bolt would have a parallelism of 1.
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
     * Sends the elements of this stream to a bolt. This could be used to plug in existing bolts as
     * sinks in the stream, for e.g. a {@code RedisStoreBolt}.
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
        // need all grouping for state query since the state is local
        Node node = all().addProcessorNode(new StateQueryProcessor<>(streamState), KEY_VALUE);
        // add 'updateState' node as parent so that state query gets processed after update state
        addNode(streamState.getUpdateStateNode(), node, node.getParallelism());
        return new PairStream<>(streamBuilder, node);
    }

    Node getNode() {
        return node;
    }

    Node addNode(Node parent, Node child, int parallelism) {
        return streamBuilder.addNode(parent, child, parallelism);
    }

    Node addNode(Node child) {
        return addNode(this.node, child);
    }

    Node addProcessorNode(Processor<?> processor, Fields outputFields) {
        return addNode(makeProcessorNode(processor, outputFields));
    }

    String getStream() {
        return stream;
    }

    private Node addNode(Node parent, Node child) {
        return streamBuilder.addNode(parent, child);
    }

    private Node addNode(Node parent, Node child, String parentStreamId) {
        return streamBuilder.addNode(parent, child, parentStreamId);
    }

    private Node addNode(Node child, int parallelism, String parentStreamId) {
        return streamBuilder.addNode(this.node, child, parallelism, parentStreamId);
    }

    private ProcessorNode makeProcessorNode(Processor<?> processor, Fields outputFields) {
        return new ProcessorNode(processor, UniqueIdGen.getInstance().getUniqueStreamId(), outputFields);
    }

    private void addSinkNode(SinkNode sinkNode, int parallelism) {
        String boltId = UniqueIdGen.getInstance().getUniqueBoltId();
        sinkNode.setComponentId(boltId);
        sinkNode.setParallelism(parallelism);
        if (node instanceof SpoutNode) {
            addNode(sinkNode, parallelism, Utils.DEFAULT_STREAM_ID);
        } else {
            /*
              * add a stream__sink stream to the current node (parent) for emitting
              * just the values (no punctuation) to the bolt.
              */
            String sinkStream = StreamUtil.getSinkStream(stream);
            node.addOutputStream(sinkStream);
            addNode(sinkNode, parallelism, sinkStream);
        }
    }

    private Stream<T> global() {
        Node partitionNode = addNode(new PartitionNode(stream, node.getOutputFields(), GroupingInfo.global()));
        return new Stream<>(streamBuilder, partitionNode);
    }

    private Stream<T> all() {
        Node partitionNode = addNode(new PartitionNode(stream, node.getOutputFields(), GroupingInfo.all()));
        return new Stream<>(streamBuilder, partitionNode);
    }
}
