/*
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

package org.apache.storm.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

import org.apache.storm.Config;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StormThreadFactoryTest {

    @Test
    public void flagOffCreatesPlatformThreadsWithPrefix() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_VIRTUAL_THREADS_ENABLED, false);
        ThreadFactory factory = StormThreadFactory.create(conf, "test-pool");

        Thread t = factory.newThread(() -> { });
        assertFalse(t.isVirtual());
        assertFalse(t.isDaemon());
        assertEquals(Thread.NORM_PRIORITY, t.getPriority());
        assertEquals("test-pool-0", t.getName());
        assertEquals("test-pool-1", factory.newThread(() -> { }).getName());
    }

    @Test
    public void missingFlagBehavesAsOff() {
        ThreadFactory factory = StormThreadFactory.create(new HashMap<>(), "test-pool");
        assertFalse(factory.newThread(() -> { }).isVirtual());
    }

    @Test
    public void flagOnCreatesVirtualThreadsWithPrefix() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_VIRTUAL_THREADS_ENABLED, true);
        ThreadFactory factory = StormThreadFactory.create(conf, "test-pool");

        Thread t = factory.newThread(() -> { });
        assertTrue(t.isVirtual());
        assertEquals("test-pool-0", t.getName());
        assertEquals("test-pool-1", factory.newThread(() -> { }).getName());
    }

    @Test
    public void flagOnAcceptsStringValue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_VIRTUAL_THREADS_ENABLED, "true");
        assertTrue(StormThreadFactory.create(conf, "test-pool").newThread(() -> { }).isVirtual());
    }

    @Test
    public void platformThreadForcesNormPriorityRegardlessOfCallerPriority() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_VIRTUAL_THREADS_ENABLED, false);
        ThreadFactory factory = StormThreadFactory.create(conf, "test-pool");

        Thread callerThread = Thread.currentThread();
        int originalPriority = callerThread.getPriority();
        try {
            callerThread.setPriority(Thread.MAX_PRIORITY);
            Thread t = factory.newThread(() -> { });
            assertEquals(Thread.NORM_PRIORITY, t.getPriority());
        } finally {
            callerThread.setPriority(originalPriority);
        }
    }

    @Test
    public void unexpectedValueTypeBehavesAsOff() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_VIRTUAL_THREADS_ENABLED, Integer.valueOf(1));
        ThreadFactory factory = StormThreadFactory.create(conf, "test-pool");
        assertFalse(factory.newThread(() -> { }).isVirtual());
    }
}
