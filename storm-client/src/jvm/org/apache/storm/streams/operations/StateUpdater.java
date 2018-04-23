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
 * Interface for updating state.
 *
 * @param <T> the value type
 * @param <S> the state type
 */
public interface StateUpdater<T, S> extends Operation {
    /**
     * A static factory to create a {@link StateUpdater} based on an initial value of the state and a state update function.
     *
     * @param initialValue  the intial value of the state
     * @param stateUpdateFn the state update function
     * @param <T>           the value type
     * @param <S>           the state type
     * @return the {@link StateUpdater}
     */
    static <T, S> StateUpdater<T, S> of(S initialValue,
                                        BiFunction<? super S, ? super T, ? extends S> stateUpdateFn) {
        return new StateUpdater<T, S>() {
            @Override
            public S init() {
                return initialValue;
            }

            @Override
            public S apply(S state, T value) {
                return stateUpdateFn.apply(state, value);
            }
        };
    }

    /**
     * The initial value of the state to start with.
     *
     * @return the initial value of the state
     */
    S init();

    /**
     * Returns a new state by applying the value on the current state.
     *
     * @param state the current state
     * @param value the value
     * @return the new state
     */
    S apply(S state, T value);
}
