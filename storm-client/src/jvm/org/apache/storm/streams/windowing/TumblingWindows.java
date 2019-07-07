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

package org.apache.storm.streams.windowing;

import static org.apache.storm.topology.base.BaseWindowedBolt.Count;
import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

import org.apache.storm.topology.base.BaseWindowedBolt;

/**
 * A tumbling window specification. The window tumbles after the specified window length.
 *
 * @param <L> the type of the length of the window (e.g Count, Duration)
 */
public class TumblingWindows<L> extends BaseWindow<L, L> {
    private final L windowLength;

    private TumblingWindows(L windowLength) {
        this.windowLength = windowLength;
    }

    /**
     * A count based tumbling window.
     *
     * @param count the number of tuples after which the window tumbles
     */
    public static TumblingWindows<Count> of(Count count) {
        return new TumblingWindows<>(count);
    }

    /**
     * A time duration based tumbling window.
     *
     * @param duration the time duration after which the window tumbles
     */
    public static TumblingWindows<Duration> of(Duration duration) {
        return new TumblingWindows<>(duration);
    }

    @Override
    public L getWindowLength() {
        return windowLength;
    }

    @Override
    public L getSlidingInterval() {
        return windowLength;
    }

    /**
     * The name of the field in the tuple that contains the timestamp when the event occurred as a long value. This is used of event-time
     * based processing. If this config is set and the field is not present in the incoming tuple, an {@link IllegalArgumentException} will
     * be thrown.
     *
     * @param fieldName the name of the field that contains the timestamp
     */
    public TumblingWindows<L> withTimestampField(String fieldName) {
        timestampField = fieldName;
        return this;
    }

    /**
     * Specify a stream id on which late tuples are going to be emitted. They are going to be accessible via the {@link
     * org.apache.storm.topology.WindowedBoltExecutor#LATE_TUPLE_FIELD} field. It must be defined on a per-component basis, and in
     * conjunction with the {@link BaseWindowedBolt#withTimestampField}, otherwise {@link IllegalArgumentException} will be thrown.
     *
     * @param streamId the name of the stream used to emit late tuples on
     */
    public TumblingWindows<L> withLateTupleStream(String streamId) {
        lateTupleStream = streamId;
        return this;
    }

    /**
     * Specify the maximum time lag of the tuple timestamp in milliseconds. It means that the tuple timestamps cannot be out of order by
     * more than this amount.
     *
     * @param duration the max lag duration
     */
    public TumblingWindows<L> withLag(Duration duration) {
        lag = duration;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        TumblingWindows<?> that = (TumblingWindows<?>) o;

        return windowLength != null ? windowLength.equals(that.windowLength) : that.windowLength == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (windowLength != null ? windowLength.hashCode() : 0);
        return result;
    }
}
