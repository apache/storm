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

package org.apache.storm.streams.processors;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Base implementation of the {@link Processor} interface that provides convenience methods {@link #execute(Object)} and {@link #finish()}.
 */
abstract class BaseProcessor<T> implements Processor<T> {
    private final Set<String> punctuationState = new HashSet<>();
    protected ProcessorContext context;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    /**
     * {@inheritDoc} Processors that do not care about the source stream should override {@link BaseProcessor#execute(Object)} instead.
     */
    @Override
    public void execute(T input, String streamId) {
        execute(input);
    }

    /**
     * Execute some operation on the input value. Sub classes can override this when then don't care about the source stream from where the
     * input is received.
     *
     * @param input the input
     */
    protected void execute(T input) {
        // NOOP
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void punctuate(String stream) {
        if ((stream == null) || shouldPunctuate(stream)) {
            finish();
            context.forward(PUNCTUATION);
            punctuationState.clear();
        }
    }

    /**
     * This is triggered to signal the end of the current batch of values. Sub classes can override this to emit the result of a batch of
     * values, for e.g. to emit the result of an aggregate or join operation on a batch of values. If a processor does per-value operation
     * like filter, map etc, they can choose to ignore this.
     */
    protected void finish() {
        // NOOP
    }

    /**
     * Forwards the result update to downstream processors. Processors that operate on a batch of tuples, like aggregation, join etc can use
     * this to emit the partial results on each input if they are operating in non-windowed mode.
     *
     * @param result the result function
     * @param <R>    the result type
     */
    protected final <R> void mayBeForwardAggUpdate(Supplier<R> result) {
        if (!context.isWindowed()) {
            context.forward(result.get());
        }
    }

    private boolean shouldPunctuate(String parentStream) {
        punctuationState.add(parentStream);
        return punctuationState.equals(context.getWindowedParentStreams());
    }
}
