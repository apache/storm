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

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of MessageRetryManager.
 */
public class DefaultMessageRetryManager implements MessageRetryManager {
    private Map<String,ConsumerMessage> cache = new ConcurrentHashMap<>(500);
    private BlockingQueue<ConsumerMessage> queue;
    private int maxRetry;
    private int ttl;

    /**
     * DefaultMessageRetryManager Constructor.
     * @param queue messages BlockingQueue
     * @param maxRetry max retry times
     * @param ttl TTL
     */
    public DefaultMessageRetryManager(BlockingQueue<ConsumerMessage> queue, int maxRetry, int ttl) {
        this.queue = queue;
        this.maxRetry = maxRetry;
        this.ttl = ttl;

        long period = 5000;
        new Timer().scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                for (Map.Entry<String, ConsumerMessage> entry : cache.entrySet()) {
                    String id = entry.getKey();
                    ConsumerMessage message = entry.getValue();
                    if (now - message.getTimestamp() >= ttl) { // no ack/fail received in ttl
                        fail(id);
                    }
                }
            }
        }, period, period);
    }

    @Override
    public void ack(String id) {
        cache.remove(id);
    }

    @Override
    public void fail(String id) {
        ConsumerMessage message = cache.remove(id);
        if (message == null) {
            return;
        }

        if (needRetry(message)) {
            message.setRetries(message.getRetries() + 1);
            message.setTimestamp(0);
            queue.offer(message);
        }
    }

    @Override
    public void mark(ConsumerMessage message) {
        message.setTimestamp(System.currentTimeMillis());
        cache.put(message.getId(), message);
    }

    @Override
    public boolean needRetry(ConsumerMessage message) {
        return message.getRetries() < maxRetry;
    }

    // just for testing
    public void setCache(Map<String,ConsumerMessage> cache) {
        this.cache = cache;
    }

}
