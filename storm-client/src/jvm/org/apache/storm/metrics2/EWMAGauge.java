/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metrics2;

import com.codahale.metrics.Gauge;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class EWMAGauge implements Gauge<Double> {

    public static final double RFC1889_ALPHA = 1.0 / 16.0;
    private static final long UNSEEDED = Long.MIN_VALUE;
    private static final int MAX_WINDOW_SIZE = 10;

    private final double alpha;
    private double jitter = 0.0;
    private final AtomicLong lastTransit = new AtomicLong(UNSEEDED);
    private final LongAdder deviationSum = new LongAdder();
    private final LongAdder deviationCount = new LongAdder();

    EWMAGauge(double alpha) {
        if (alpha <= 0.0 || alpha >= 1.0 || Double.isNaN(alpha)) {
            throw new IllegalArgumentException(
                    "alpha must be in (0, 1), got: " + alpha);
        }
        this.alpha = alpha;
    }

    EWMAGauge() {
        this(RFC1889_ALPHA);
    }

    public void addValue(long transitMs) {
        if (transitMs < 0) {
            return;
        }
        if (lastTransit.compareAndSet(UNSEEDED, transitMs)) {
            return;
        }
        long prev = lastTransit.getAndSet(transitMs);
        if (prev == UNSEEDED) {
            return;
        }
        deviationSum.add(Math.abs(transitMs - prev));
        deviationCount.increment();
        if (deviationCount.longValue() >= MAX_WINDOW_SIZE) {
            getValue();
        }
    }

    @Override
    public synchronized Double getValue() {
        long sum = deviationSum.sumThenReset();
        long count = deviationCount.sumThenReset();
        if (count > 0) {
            double meanDeviation = (double) sum / count;
            jitter += (meanDeviation - jitter) * alpha;
        }
        return jitter;
    }
}