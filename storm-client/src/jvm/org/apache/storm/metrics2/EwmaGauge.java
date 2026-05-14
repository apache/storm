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

package org.apache.storm.metrics2;

import static org.apache.storm.utils.ConfigUtils.RFC1889_ALPHA;

import com.codahale.metrics.Gauge;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock-free jitter estimator following RFC 1889 §A.8 / RFC 3550 §A.8.
 * The jitter accumulator is stored as raw IEEE 754 bits in an AtomicLong
 * so that CAS can be used without locks.
 * Thread safety: addValue is lock-free; getValue is wait-free.
 */
public class EwmaGauge implements Gauge<Double> {

    private static final long UNSEEDED = Long.MIN_VALUE;
    private static final long ZERO_BITS = Double.doubleToLongBits(0.0);

    private final AtomicLong lastTransit = new AtomicLong(UNSEEDED);
    private final AtomicLong jitterBits = new AtomicLong(ZERO_BITS);
    private final double alpha;

    EwmaGauge(double alpha) {
        if (alpha <= 0.0 || alpha >= 1.0 || Double.isNaN(alpha)) {
            throw new IllegalArgumentException(
                    "alpha must be in (0, 1), got: " + alpha);
        }
        this.alpha = alpha;
    }

    EwmaGauge() {
        this(RFC1889_ALPHA);  // 1.0 / 16.0
    }

    /**
     * Update the jitter estimate.
     *
     * @param transitMs transit time for this tuple: {@code arrival - timestamp}
     *                  Negative values are silently ignored.
     */
    public void addValue(long transitMs) {
        if (transitMs < 0) {
            return;
        }
        // Seed on the very first packet: store transit, nothing to diff against yet.
        if (lastTransit.compareAndSet(UNSEEDED, transitMs)) {
            return;
        }
        long prev = lastTransit.getAndSet(transitMs);
        // Safe from Math.abs(Long.MIN_VALUE) pathology: both transitMs and prev
        // are >= 0 (enforced by the negative-guard), so their
        // difference is in [-Long.MAX_VALUE, Long.MAX_VALUE].
        double d = Math.abs(transitMs - prev);
        long currentBits;
        long updatedBits;
        do {
            currentBits = jitterBits.get();
            double currentJitter = Double.longBitsToDouble(currentBits);
            double updatedJitter = currentJitter + alpha * (d - currentJitter);
            updatedBits = Double.doubleToLongBits(updatedJitter);
        } while (!jitterBits.compareAndSet(currentBits, updatedBits));
    }

    /**
     * Returns the current jitter estimate in timestamp units.
     */
    @Override
    public Double getValue() {
        return Double.longBitsToDouble(jitterBits.get());
    }
}
