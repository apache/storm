/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.utils;

import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RotatingMap#peekOldestBucket()}, added for STORM-2359.
 */
public class RotatingMapPeekTest {

    /**
     * A freshly constructed RotatingMap should have an empty oldest bucket.
     */
    @Test
    public void testPeekOldestBucketEmptyOnCreation() {
        RotatingMap<String, Integer> map = new RotatingMap<>(3);
        Map<String, Integer> oldest = map.peekOldestBucket();
        assertTrue(oldest.isEmpty(), "Oldest bucket should be empty on a new RotatingMap");
    }

    /**
     * After putting a key, it lands in the head (newest) bucket, not the oldest.
     * The oldest bucket should remain empty until a rotation occurs.
     */
    @Test
    public void testPeekOldestBucketDoesNotContainFreshlyPutKey() {
        RotatingMap<String, Integer> map = new RotatingMap<>(3);
        map.put("key1", 1);
        Map<String, Integer> oldest = map.peekOldestBucket();
        assertTrue(oldest.isEmpty(),
            "Freshly put key should be in head bucket, not oldest bucket");
    }

    /**
     * After (numBuckets - 1) rotations, the key that was put before the first
     * rotation should appear in the oldest bucket.
     */
    @Test
    public void testPeekOldestBucketContainsKeyAfterRotations() {
        int numBuckets = 3;
        RotatingMap<String, Integer> map = new RotatingMap<>(numBuckets);

        map.put("key1", 42);

        // After (numBuckets - 1) rotations the key reaches the oldest bucket
        for (int i = 0; i < numBuckets - 1; i++) {
            map.rotate();
        }

        Map<String, Integer> oldest = map.peekOldestBucket();
        assertTrue(oldest.containsKey("key1"),
            "key1 should be in oldest bucket after " + (numBuckets - 1) + " rotations");
        assertEquals(42, oldest.get("key1"));
    }

    /**
     * After a key migrates to the oldest bucket, calling put() on it (which
     * moves it back to the head bucket) should cause peekOldestBucket() to
     * no longer contain it.
     */
    @Test
    public void testPeekOldestBucketAfterReinsertion() {
        int numBuckets = 3;
        RotatingMap<String, Integer> map = new RotatingMap<>(numBuckets);

        map.put("key1", 10);

        for (int i = 0; i < numBuckets - 1; i++) {
            map.rotate();
        }

        // Confirm key is in oldest bucket
        assertTrue(map.peekOldestBucket().containsKey("key1"));

        // Re-insert — this is what rescueRecentlyActiveEntries() does in Acker
        map.put("key1", 10);

        // Oldest bucket should no longer contain key1
        assertTrue(map.peekOldestBucket().isEmpty() || !map.peekOldestBucket().containsKey("key1"),
            "key1 should have been moved out of oldest bucket after re-insertion");
    }

    /**
     * The returned map must be unmodifiable — attempting mutation must throw.
     */
    @Test
    public void testPeekOldestBucketIsUnmodifiable() {
        int numBuckets = 3;
        RotatingMap<String, Integer> map = new RotatingMap<>(numBuckets);
        map.put("key1", 1);

        for (int i = 0; i < numBuckets - 1; i++) {
            map.rotate();
        }

        Map<String, Integer> oldest = map.peekOldestBucket();
        assertThrows(UnsupportedOperationException.class,
            () -> oldest.put("hacked", 999),
            "peekOldestBucket() must return an unmodifiable view");
    }

    /**
     * After a full rotation (numBuckets rotations), an entry that was never
     * re-inserted should be evicted and no longer visible via peekOldestBucket.
     */
    @Test
    public void testPeekOldestBucketAfterEviction() {
        int numBuckets = 3;
        RotatingMap<String, Integer> map = new RotatingMap<>(numBuckets);

        map.put("key1", 5);

        // One extra rotation evicts key1
        for (int i = 0; i < numBuckets; i++) {
            map.rotate();
        }

        // key1 is now gone from all buckets, oldest bucket should be empty
        assertTrue(map.peekOldestBucket().isEmpty(),
            "Oldest bucket should be empty after key has been evicted");
    }
}
