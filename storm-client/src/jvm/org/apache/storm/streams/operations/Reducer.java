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
 * The {@link Reducer} performs an operation on two values of the same type producing a result of the same type.
 *
 * @param <T> the type of the arguments and the result
 */
public interface Reducer<T> extends BiFunction<T, T, T> {
    /**
     * Applies this function to the given arguments.
     *
     * @param arg1 the first argument
     * @param arg2 the second argument
     * @return the result
     */
    @Override
    T apply(T arg1, T arg2);
}
