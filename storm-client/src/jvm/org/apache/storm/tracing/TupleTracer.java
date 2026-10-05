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

package org.apache.storm.tracing;

import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Tuple;

/**
 * Attaches a trace context to tuples and records how their tuple trees are processed. Each worker creates one instance from the
 * class named in {@link Config#TOPOLOGY_TRACING_TRACER}, through its zero-arg constructor, and shares it between its executors
 * and the threads that serialize and deserialize tuples, so implementations must be thread safe.
 *
 * <p>A context is any object the implementation chooses; null means not traced. Storm keeps it on the tuple and, for the tuple
 * trees a spout starts, until the tree completes. Tuples sent to another worker carry the bytes {@link #encode} returns, and the
 * receiving worker turns them back into a context with {@link #decode}.
 *
 * <p>An exception from {@link #decode} leaves the tuple without a context. Exceptions from the other methods propagate to the
 * caller.
 */
public interface TupleTracer {

    /**
     * Called once, before any other method.
     */
    void prepare(Map<String, Object> topoConf, WorkerTopologyContext context);

    /**
     * Returns the context of a tuple tree that spout task {@code taskId} starts on {@code streamId}, or null to leave it untraced.
     * Not called for checkpoint tuples.
     */
    Object spoutEmit(int taskId, String streamId);

    /**
     * Returns the context of a tuple that bolt task {@code taskId} emits on {@code streamId}, or null. {@code anchorContexts} holds
     * the contexts of the anchors that carry one, in anchor order; it is empty when none does or the emit is unanchored. Called on
     * the emitting thread.
     */
    Object boltEmit(int taskId, String streamId, List<Object> anchorContexts);

    /**
     * Called on the executor thread before bolt task {@code taskId} runs {@code execute()} for {@code tuple}, which carries
     * {@code context}. Storm puts {@link ExecuteScope#context()} on the tuple, then closes the scope on the same thread once
     * {@code execute()} returns or throws. Returns null to run {@code execute()} without a scope; the tuple keeps {@code context}.
     */
    ExecuteScope startExecute(int taskId, Tuple tuple, Object context);

    /**
     * Called on the executor thread of spout task {@code taskId} after the tuple tree of {@code context} was acked, failed or timed
     * out, and the spout's {@code ack()} or {@code fail()} returned. {@code latencyMs} is the time from the emit of the tree to
     * its outcome, measured before the spout's {@code ack()} or {@code fail()} runs. Not called for trees that no acker tracks,
     * such as all trees of a topology without ackers.
     */
    void spoutOutcome(int taskId, Object context, Outcome outcome, long latencyMs);

    /**
     * Called when bolt task {@code taskId} fails a tuple that carries {@code context}, on the thread that calls {@code fail()}.
     */
    void boltFail(int taskId, Object context);

    /**
     * Returns the bytes that carry {@code context} to another worker, or null to send the tuple without it.
     */
    byte[] encode(Object context);

    /**
     * Returns the context in bytes that {@link #encode} returned on a worker of the same topology, or null.
     */
    Object decode(byte[] bytes);

    /**
     * How a spout tuple tree ended.
     */
    enum Outcome {
        ACK,
        FAIL,
        TIMEOUT
    }

    /**
     * The tracing state of one {@code execute()} call.
     */
    interface ExecuteScope extends AutoCloseable {
        /**
         * Returns the context the tuple carries while and after {@code execute()} runs.
         */
        Object context();

        @Override
        void close();
    }
}
