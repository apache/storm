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

import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

import java.io.Serializable;

/**
 * The window specification within {@link org.apache.storm.streams.Stream}.
 *
 * @param <L> the type of window length parameter (E.g. Count, Duration)
 * @param <I> the type of the sliding interval parameter (E.g. Count, Duration)
 */
public interface Window<L, I> extends Serializable {

    /**
     * The length of the window.
     *
     * @return the window length
     */
    L getWindowLength();

    /**
     * The sliding interval of the window.
     *
     * @return the sliding interval
     */
    I getSlidingInterval();

    /**
     * The name of the field in the tuple that contains the timestamp when the event occurred as a long value. This is used of event-time
     * based processing. If this config is set and the field is not present in the incoming tuple, an {@link IllegalArgumentException} will
     * be thrown.
     *
     * @return the timestamp field.
     */
    String getTimestampField();

    /**
     * The name of the stream where late arriving tuples should be emitted. If this is not provided, the late tuples would be discarded.
     *
     * @return the name of the stream used to emit late tuples on
     */
    String getLateTupleStream();

    /**
     * The maximum time lag of the tuple timestamp in milliseconds. It means that the tuple timestamps cannot be out of order by more than
     * this amount.
     *
     * @return the lag
     */
    Duration getLag();
}
