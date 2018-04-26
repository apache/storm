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

package org.apache.storm.topology.base;

import org.apache.storm.Config;
import org.apache.storm.state.State;
import org.apache.storm.topology.IStatefulWindowedBolt;
import org.apache.storm.windowing.TimestampExtractor;

public abstract class BaseStatefulWindowedBolt<T extends State> extends BaseWindowedBolt implements IStatefulWindowedBolt<T> {
    // if the windows should be persisted in state
    private boolean persistent;

    // max number of window events in memory
    private long maxEventsInMemory;

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Count windowLength, Count slidingInterval) {
        super.withWindow(windowLength, slidingInterval);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Count windowLength, Duration slidingInterval) {
        super.withWindow(windowLength, slidingInterval);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Duration windowLength, Count slidingInterval) {
        super.withWindow(windowLength, slidingInterval);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Duration windowLength, Duration slidingInterval) {
        super.withWindow(windowLength, slidingInterval);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Count windowLength) {
        super.withWindow(windowLength);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Duration windowLength) {
        super.withWindow(windowLength);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withTumblingWindow(Count count) {
        super.withTumblingWindow(count);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withTumblingWindow(Duration duration) {
        super.withTumblingWindow(duration);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withTimestampField(String fieldName) {
        super.withTimestampField(fieldName);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withTimestampExtractor(TimestampExtractor timestampExtractor) {
        super.withTimestampExtractor(timestampExtractor);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withLateTupleStream(String streamName) {
        super.withLateTupleStream(streamName);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withLag(Duration duration) {
        super.withLag(duration);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWatermarkInterval(Duration interval) {
        super.withWatermarkInterval(interval);
        return this;
    }

    /**
     * Specify the name of the field in the tuple that holds the message id. This is used to track the windowing boundaries and
     * re-evaluating the windowing operation during recovery of IStatefulWindowedBolt
     *
     * @param fieldName the name of the field that contains the message id
     */
    public BaseStatefulWindowedBolt<T> withMessageIdField(String fieldName) {
        windowConfiguration.put(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME, fieldName);
        return this;
    }

    /**
     * If set, the stateful windowed bolt would use the backend state for window persistence and only keep a sub-set of events in memory as
     * specified by {@link #withMaxEventsInMemory(long)}.
     */
    public BaseStatefulWindowedBolt<T> withPersistence() {
        persistent = true;
        return this;
    }

    /**
     * The maximum number of window events to keep in memory. This is meaningful only if {@link #withPersistence()} is also set. As the
     * number of events in memory grows close to the maximum, the events that are less likely to be used again are evicted and persisted.
     * The default value for this is {@code 1,000,000}.
     *
     * @param maxEventsInMemory the maximum number of window events to keep in memory
     */
    public BaseStatefulWindowedBolt<T> withMaxEventsInMemory(long maxEventsInMemory) {
        this.maxEventsInMemory = maxEventsInMemory;
        return this;
    }

    @Override
    public boolean isPersistent() {
        return persistent;
    }

    @Override
    public long maxEventsInMemory() {
        return maxEventsInMemory > 0 ? maxEventsInMemory : IStatefulWindowedBolt.super.maxEventsInMemory();
    }

    @Override
    public void preCommit(long txid) {
        // NOOP
    }

    @Override
    public void prePrepare(long txid) {
        // NOOP
    }

    @Override
    public void preRollback() {
        // NOOP
    }
}
