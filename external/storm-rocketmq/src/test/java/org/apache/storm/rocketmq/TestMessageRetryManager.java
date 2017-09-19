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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.storm.utils.Utils;
import org.junit.Before;
import org.junit.Test;

public class TestMessageRetryManager {
    MessageRetryManager messageRetryManager;
    Map<String, ConsumerMessage> cache;
    BlockingQueue<ConsumerMessage> queue;

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
        ConsumerMessage message = new ConsumerMessage("id", new MessageExt());
        messageRetryManager.mark(message);
        assertEquals(1, cache.size());
        assertTrue(cache.containsKey(message.getId()));

        messageRetryManager.ack(message.getId());
        assertEquals(0, cache.size());
        assertFalse(cache.containsKey(message.getId()));


        //fail need retry: retries < maxRetry
        message = new ConsumerMessage("id", new MessageExt());
        messageRetryManager.mark(message);
        assertEquals(1, cache.size());
        assertTrue(cache.containsKey(message.getId()));

        messageRetryManager.fail(message.getId());
        assertEquals(0, cache.size());
        assertFalse(cache.containsKey(message.getId()));
        assertEquals(1, message.getRetries());
        assertEquals(1, queue.size());
        assertEquals(message, queue.poll());


        //fail need not retry: retries >= maxRetry
        message = new ConsumerMessage("id", new MessageExt());
        messageRetryManager.mark(message);
        messageRetryManager.fail(message.getId());
        assertEquals(0, cache.size());
        assertFalse(cache.containsKey(message.getId()));

        messageRetryManager.mark(message);
        messageRetryManager.fail(message.getId());
        assertEquals(2, message.getRetries());
        messageRetryManager.mark(message);
        messageRetryManager.fail(message.getId());
        assertEquals(3, message.getRetries());

        assertFalse(messageRetryManager.needRetry(message));
        messageRetryManager.mark(message);
        messageRetryManager.fail(message.getId());
        assertEquals(0, cache.size());
        assertEquals(3, queue.size());
        assertEquals(message, queue.poll());


        //fail: no ack/fail received in ttl
        message = new ConsumerMessage("id", new MessageExt());
        messageRetryManager.mark(message);
        Utils.sleep(10000);
        assertEquals(0, cache.size());
        assertFalse(cache.containsKey(message.getId()));

    }
}
