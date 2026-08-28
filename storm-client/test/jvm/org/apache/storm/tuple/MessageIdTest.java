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

package org.apache.storm.tuple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.storm.utils.KeyStreamRandom;
import org.junit.jupiter.api.Test;

public class MessageIdTest {

    private static final long LCG_MULTIPLIER = 0x5DEECE66DL;
    private static final long LCG_ADDEND = 0xBL;
    private static final long LCG_MASK = (1L << 48) - 1;

    @Test
    public void generateIdReturnsDistinctValues() {
        Random rand = new KeyStreamRandom();
        Set<Long> ids = new HashSet<>();
        for (int i = 0; i < 100000; i++) {
            ids.add(MessageId.generateId(rand));
        }
        assertEquals(100000, ids.size());
    }

    @Test
    public void generatedIdsDoNotRevealTheFollowingIds() {
        Random rand = new KeyStreamRandom();
        for (int i = 0; i < 20; i++) {
            long first = MessageId.generateId(rand);
            long second = MessageId.generateId(rand);
            assertNotEquals(second, predictNextFromLcgState(first));
        }
    }

    /**
     * Guards the test above: the same prediction does work against a java.util.Random, so a failure of
     * generatedIdsDoNotRevealTheFollowingIds means the ids really are predictable rather than the prediction being broken.
     */
    @Test
    public void lcgPredictionWorksAgainstJavaUtilRandom() {
        Random rand = new Random(4242L);
        for (int i = 0; i < 20; i++) {
            long first = MessageId.generateId(rand);
            long second = MessageId.generateId(rand);
            assertEquals(second, predictNextFromLcgState(first));
        }
    }

    @Test
    public void makeRootIdKeepsTheGeneratedIds() {
        Random rand = new KeyStreamRandom();
        long id = MessageId.generateId(rand);
        long val = MessageId.generateId(rand);
        MessageId messageId = MessageId.makeRootId(id, val);
        assertEquals(val, messageId.getAnchorsToIds().get(id));
        assertNotNull(messageId.toString());
    }

    /**
     * Recovers the 48 bit state of a java.util.Random from a single nextLong output and returns the nextLong it would produce
     * afterwards, or 0 if the state could not be recovered.
     */
    private static long predictNextFromLcgState(long observed) {
        int low = (int) observed;
        int high = (int) ((observed - low) >>> 32);
        long partialSeed = (high & 0xFFFFFFFFL) << 16;
        for (int guess = 0; guess < (1 << 16); guess++) {
            long seed = nextSeed(partialSeed | guess);
            if ((int) (seed >>> 16) == low) {
                seed = nextSeed(seed);
                long nextHigh = seed >>> 16;
                seed = nextSeed(seed);
                long nextLow = seed >>> 16;
                return (nextHigh << 32) + (int) nextLow;
            }
        }
        return 0;
    }

    private static long nextSeed(long seed) {
        return (seed * LCG_MULTIPLIER + LCG_ADDEND) & LCG_MASK;
    }
}
