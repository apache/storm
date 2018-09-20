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

package org.apache.storm.streams.operations;

/**
 * Interface for aggregating values.
 *
 * @param <T> the original value type
 * @param <A> the accumulator type
 * @param <R> the result type
 */
public interface CombinerAggregator<T, A, R> extends Operation {
    /**
     * A static factory to create a {@link CombinerAggregator} based on initial value, accumulator and combiner.
     *
     * @param initialValue the initial value of the result to start with
     * @param accumulator  a function that accumulates values into a partial result
     * @param combiner     a function that combines partially accumulated results
     * @param <T>          the value type
     * @param <R>          the result type
     * @return the {@link CombinerAggregator}
     */
    static <T, R> CombinerAggregator<T, R, R> of(R initialValue,
                                                 BiFunction<? super R, ? super T, ? extends R> accumulator,
                                                 BiFunction<? super R, ? super R, ? extends R> combiner) {
        return new CombinerAggregator<T, R, R>() {
            @Override
            public R init() {
                return initialValue;
            }

            @Override
            public R apply(R aggregate, T value) {
                return accumulator.apply(aggregate, value);
            }

            @Override
            public R merge(R accum1, R accum2) {
                return combiner.apply(accum1, accum2);
            }

            @Override
            public R result(R accum) {
                return accum;
            }
        };
    }

    /**
     * The initial value of the accumulator to start with.
     *
     * @return the initial value of the accumulator
     */
    A init();

    /**
     * Updates the accumulator by applying the current accumulator with the value.
     *
     * @param accumulator the current accumulator
     * @param value       the value
     * @return the updated accumulator
     */
    A apply(A accumulator, T value);

    /**
     * Merges two accumulators and returns the merged accumulator.
     *
     * @param accum1 the first accumulator
     * @param accum2 the second accumulator
     * @return the merged accumulator
     */
    A merge(A accum1, A accum2);

    /**
     * Produces a result value out of the accumulator.
     *
     * @param accum the accumulator
     * @return the result
     */
    R result(A accum);

}
