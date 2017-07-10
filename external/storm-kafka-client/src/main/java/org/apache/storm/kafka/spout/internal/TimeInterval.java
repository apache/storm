/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout.internal;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class TimeInterval implements Serializable {
    private final long lengthNanos;
    private final TimeUnit timeUnit;
    private final long length;

    /**
     * @param length length of the time interval in the units specified by {@link TimeUnit}
     * @param timeUnit unit used to specify a time interval on which to specify a time unit
     */
    public TimeInterval(long length, TimeUnit timeUnit) {
        this.lengthNanos = timeUnit.toNanos(length);
        this.timeUnit = timeUnit;
        this.length = length;
    }

    public static TimeInterval seconds(long length) {
        return new TimeInterval(length, TimeUnit.SECONDS);
    }

    public static TimeInterval milliSeconds(long length) {
        return new TimeInterval(length, TimeUnit.MILLISECONDS);
    }

    public static TimeInterval microSeconds(long length) {
        return new TimeInterval(length, TimeUnit.MICROSECONDS);
    }

    public long getLengthNanos() {
        return lengthNanos;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public long getLength() {
        return length;
    }

    @Override
    public String toString() {
        return "TimeInterval{" +
                "length=" + length +
                ", timeUnit=" + timeUnit +
                '}';
    }
}
