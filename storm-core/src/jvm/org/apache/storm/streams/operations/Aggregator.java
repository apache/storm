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
package org.apache.storm.streams.operations;

/**
 * Interface for aggregating values.
 *
 * @param <T> the original value type
 * @param <R> the aggregated value type
 */
public interface Aggregator<T, R> extends Operation {
    /**
     * The initial value of the aggregate to start with.
     *
     * @return the initial value
     */
    R init();

    /**
     * Returns a new aggregate by applying the value with the current aggregate.
     *
     * @param value     the value to aggregate
     * @param aggregate the current aggregate
     * @return the new aggregate
     */
    R apply(T value, R aggregate);
}
