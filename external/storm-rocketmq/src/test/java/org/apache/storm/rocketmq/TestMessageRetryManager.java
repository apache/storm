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
package org.apache.storm.rocketmq;

import org.apache.storm.utils.Utils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestMessageRetryManager {
    MessageRetryManager messageRetryManager;
    Map<String,MessageSet> cache;
    BlockingQueue<MessageSet> queue;

    @Before
    public void prepare() {
        cache = new ConcurrentHashMap<>(10);
        queue = new LinkedBlockingDeque<>(10);
        int maxRetry = 3;
        int ttl = 2000;
        messageRetryManager = new DefaultMessageRetryManager(queue, maxRetry, ttl);
        ((DefaultMessageRetryManager)messageRetryManager).setCache(cache);
    }

    @Test
    public void testRetryLogics() {
        //ack
        MessageSet messageSet = new MessageSet(new ArrayList<>());
        messageRetryManager.mark(messageSet);
        assertEquals(1, cache.size());
        assertTrue(cache.containsKey(messageSet.getId()));

        messageRetryManager.ack(messageSet.getId());
        assertEquals(0, cache.size());
        assertFalse(cache.containsKey(messageSet.getId()));


        //fail need retry: retries < maxRetry
        messageSet = new MessageSet(new ArrayList<>());
        messageRetryManager.mark(messageSet);
        assertEquals(1, cache.size());
        assertTrue(cache.containsKey(messageSet.getId()));

        messageRetryManager.fail(messageSet.getId());
        assertEquals(0, cache.size());
        assertFalse(cache.containsKey(messageSet.getId()));
        assertEquals(1, messageSet.getRetries());
        assertEquals(1, queue.size());
        assertEquals(messageSet, queue.poll());


        //fail need not retry: retries >= maxRetry
        messageSet = new MessageSet(new ArrayList<>());
        messageRetryManager.mark(messageSet);
        messageRetryManager.fail(messageSet.getId());
        assertEquals(0, cache.size());
        assertFalse(cache.containsKey(messageSet.getId()));

        messageRetryManager.mark(messageSet);
        messageRetryManager.fail(messageSet.getId());
        assertEquals(2, messageSet.getRetries());
        messageRetryManager.mark(messageSet);
        messageRetryManager.fail(messageSet.getId());
        assertEquals(3, messageSet.getRetries());

        assertFalse(messageRetryManager.needRetry(messageSet));
        messageRetryManager.mark(messageSet);
        messageRetryManager.fail(messageSet.getId());
        assertEquals(0, cache.size());
        assertEquals(3, queue.size());
        assertEquals(messageSet, queue.poll());


        //fail: no ack/fail received in ttl
        messageSet = new MessageSet(new ArrayList<>());
        messageRetryManager.mark(messageSet);
        Utils.sleep(10000);
        assertEquals(0, cache.size());
        assertFalse(cache.containsKey(messageSet.getId()));

    }
}
