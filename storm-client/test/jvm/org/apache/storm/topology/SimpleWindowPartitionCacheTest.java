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

package org.apache.storm.topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.FutureTask;
import org.apache.storm.utils.Utils;
import org.apache.storm.windowing.persistence.SimpleWindowPartitionCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SimpleWindowPartitionCache}
 */
public class SimpleWindowPartitionCacheTest {

    @BeforeEach
    public void setUp() throws Exception {
    }

    @Test
    public void testBuildInvalid1() {
        assertThrows(IllegalArgumentException.class, () -> SimpleWindowPartitionCache.<Integer, Integer>newBuilder()
            .maximumSize(0)
            .build(null));
    }

    @Test
    public void testBuildInvalid2() {
        assertThrows(IllegalArgumentException.class, () -> SimpleWindowPartitionCache.<Integer, Integer>newBuilder()
            .maximumSize(-1)
            .build(null));
    }

    @Test
    public void testBuildInvalid3() {
        assertThrows(NullPointerException.class,
            () -> SimpleWindowPartitionCache.<Integer, Integer>newBuilder()
            .maximumSize(1)
            .build(null));
    }

    @Test
    public void testBuildOk() {
        SimpleWindowPartitionCache.<Integer, Integer>newBuilder()
            .maximumSize(1)
            .removalListener((key, val, removalCause) -> {
            })
            .build(key -> key);
    }

    @Test
    public void testGet() {
        List<Integer> removed = new ArrayList<>();
        List<Integer> loaded = new ArrayList<>();
        SimpleWindowPartitionCache<Integer, Integer> cache =
            SimpleWindowPartitionCache.<Integer, Integer>newBuilder()
                .maximumSize(2)
                .removalListener((key, val, removalCause) -> removed.add(key))
                .build(key -> {
                    loaded.add(key);
                    return key;
                });

        cache.get(1);
        cache.get(2);
        cache.get(3);
        assertEquals(Arrays.asList(1, 2, 3), loaded);
        // since 2 is the largest un-pinned entry before 3 is loaded
        assertEquals(Collections.singletonList(2), removed);
    }

    @Test
    public void testGetNull() {
        assertThrows(NullPointerException.class, () -> {
            SimpleWindowPartitionCache<Integer, Integer> cache =
                SimpleWindowPartitionCache.<Integer, Integer>newBuilder()
                    .maximumSize(2)
                    .build(key -> null);

            cache.get(1);
        });
    }

    @Test
    public void testEvictNoRemovalListener() {
        SimpleWindowPartitionCache<Integer, Integer> cache =
            SimpleWindowPartitionCache.<Integer, Integer>newBuilder()
                .maximumSize(1)
                .build(key -> key);
        cache.get(1);
        cache.get(2);
        assertEquals(Collections.singletonMap(2, 2), cache.asMap());
        cache.invalidate(2);
        assertEquals(Collections.emptyMap(), cache.asMap());
    }

    @Test
    public void testPinAndGet() {
        List<Integer> removed = new ArrayList<>();
        List<Integer> loaded = new ArrayList<>();
        SimpleWindowPartitionCache<Integer, Integer> cache =
            SimpleWindowPartitionCache.<Integer, Integer>newBuilder()
                .maximumSize(1)
                .removalListener((key, val, removalCause) -> removed.add(key))
                .build(key -> {
                    loaded.add(key);
                    return key;
                });

        cache.get(1);
        cache.pinAndGet(2);
        cache.get(3);
        assertEquals(Arrays.asList(1, 2, 3), loaded);
        assertEquals(Collections.singletonList(1), removed);
    }

    @Test
    public void testInvalidate() {
        List<Integer> removed = new ArrayList<>();
        List<Integer> loaded = new ArrayList<>();
        SimpleWindowPartitionCache<Integer, Integer> cache =
            SimpleWindowPartitionCache.<Integer, Integer>newBuilder()
                .maximumSize(1)
                .removalListener((key, val, removalCause) -> removed.add(key))
                .build(key -> {
                    loaded.add(key);
                    return key;
                });

        cache.pinAndGet(1);
        cache.invalidate(1);
        assertEquals(Collections.singletonList(1), loaded);
        assertEquals(Collections.emptyList(), removed);
        assertEquals(cache.asMap(), Collections.singletonMap(1, 1));

        cache.unpin(1);
        cache.invalidate(1);
        assertTrue(cache.asMap().isEmpty());
    }


    @Timeout(10000)
    @Test
    public void testConcurrentGet() throws Exception {
        List<Integer> loaded = new ArrayList<>();
        SimpleWindowPartitionCache<Integer, Object> cache =
            SimpleWindowPartitionCache.<Integer, Object>newBuilder()
                .maximumSize(1)
                .build(key -> {
                    Utils.sleep(1000);
                    loaded.add(key);
                    return new Object();
                });

        FutureTask<Object> ft1 = new FutureTask<>(() -> cache.pinAndGet(1));
        FutureTask<Object> ft2 = new FutureTask<>(() -> cache.pinAndGet(1));
        Thread t1 = new Thread(ft1);
        Thread t2 = new Thread(ft2);
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        assertEquals(Collections.singletonList(1), loaded);
        assertEquals(ft1.get(), ft2.get());
    }

    @Test
    public void testConcurrentUnpin() throws Exception {
        SimpleWindowPartitionCache<Integer, Object> cache =
            SimpleWindowPartitionCache.<Integer, Object>newBuilder()
                .maximumSize(1)
                .build(key -> new Object());

        cache.pinAndGet(1);
        FutureTask<Boolean> ft1 = new FutureTask<>(() -> cache.unpin(1));
        FutureTask<Boolean> ft2 = new FutureTask<>(() -> cache.unpin(1));
        Thread t1 = new Thread(ft1);
        Thread t2 = new Thread(ft2);
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        assertTrue(ft1.get() || ft2.get());
        assertFalse(ft1.get() && ft2.get());
    }

    @Test
    public void testEviction() {
        List<Integer> removed = new ArrayList<>();
        SimpleWindowPartitionCache<Integer, Object> cache =
            SimpleWindowPartitionCache.<Integer, Object>newBuilder()
                .maximumSize(1)
                .removalListener((key, val, removalCause) -> removed.add(key))
                .build(key -> new Object());

        cache.get(0);
        cache.pinAndGet(1);
        assertEquals(Collections.singletonList(0), removed);
        cache.get(2);
        assertEquals(Collections.singletonList(0), removed);
    }
}