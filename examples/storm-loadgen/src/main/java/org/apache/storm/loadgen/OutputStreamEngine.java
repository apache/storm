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

package org.apache.storm.loadgen;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Provides an API to simulate the output of a stream.
 * <p>
 * Right now it is just rate, but in the future we expect to do data skew as well...
 * </p>
 */
public class OutputStreamEngine {
    private static final double NANO_PER_SEC = 1_000_000_000.0;
    private static final long UPDATE_RATE_PERIOD_NS = ((long) NANO_PER_SEC * 2);
    private static final String[] KEYS = new String[2048];

    static {
        //We get a new random number and seed it to make sure that runs are consistent where possible.
        Random r = new Random(KEYS.length);
        for (int i = 0; i < KEYS.length; i++) {
            KEYS[i] = String.valueOf(r.nextDouble());
        }
    }

    private long periodNano;
    private long emitAmount;
    private final Random rand;
    private long nextEmitTime;
    private long nextRateRandomizeTime;
    private long emitsLeft;
    private final OutputStream stats;
    public final String streamName;

    /**
     * Create an engine that can simulate the given stats.
     * @param stats the stats to follow
     */
    public OutputStreamEngine(OutputStream stats) {
        this.stats = stats;
        rand = ThreadLocalRandom.current();
        selectNewRate();
        //Start emitting right now
        nextEmitTime = System.nanoTime();
        nextRateRandomizeTime = nextEmitTime + UPDATE_RATE_PERIOD_NS;
        emitsLeft = emitAmount;
        streamName = stats.id;
    }

    private void selectNewRate() {
        double ratePerSecond = stats.rate.nextRandom(rand);
        if (ratePerSecond > 0) {
            periodNano = Math.max(1, (long) (NANO_PER_SEC / ratePerSecond));
            emitAmount = Math.max(1, (long) ((ratePerSecond / NANO_PER_SEC) * periodNano));
        } else {
            //if it is is 0 or less it really is 1 per 10 seconds.
            periodNano = (long) NANO_PER_SEC * 10;
            emitAmount = 1;
        }
    }

    /**
     * Should we emit or not.
     * @return the start time of the message, or null of nothing should be emitted.
     */
    public Long shouldEmit() {
        long time = System.nanoTime();
        if (emitsLeft <= 0 && nextEmitTime <= time) {
            emitsLeft = emitAmount;
            nextEmitTime = nextEmitTime + periodNano;
        }

        if (nextRateRandomizeTime <= time) {
            //Once every UPDATE_RATE_PERIOD_NS
            selectNewRate();
            nextRateRandomizeTime = nextEmitTime + UPDATE_RATE_PERIOD_NS;
        }

        if (emitsLeft > 0) {
            emitsLeft--;
            return nextEmitTime - periodNano;
        }
        return null;
    }

    /**
     * Get the next key to emit.
     * @return the key that should be emitted.
     */
    public String nextKey() {
        int keyIndex;
        if (stats.areKeysSkewed) {
            //We set the stddev of the skewed keys to be 1/5 of the length, but then we use the absolute value
            // of that so everything is skewed towards 0
            keyIndex = Math.min(KEYS.length - 1 , Math.abs((int) (rand.nextGaussian() * KEYS.length / 5)));
        } else {
            keyIndex = rand.nextInt(KEYS.length);
        }
        return KEYS[keyIndex];
    }

    public int nextInt(int bound) {
        return rand.nextInt(bound);
    }
}