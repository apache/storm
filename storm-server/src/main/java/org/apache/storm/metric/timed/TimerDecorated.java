/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.storm.metric.timed;

import com.codahale.metrics.Timer;

public interface TimerDecorated extends AutoCloseable {

    long stopTiming();

    /**
     * Stop the timer for measured object. (Copied from {@link Timer.Context#stop()})
     * Call to this method will not reset the start time.
     * Multiple calls result in multiple updates.
     *
     * @return Time a object is in use, or under measurement, in nanoseconds.
     */
    default long stopTiming(final Timer.Context timing) {
        return timing.stop();
    }

    @Override
    default void close() throws Exception {
        stopTiming();
    }
}
