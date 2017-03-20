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
 * An implementation of MessageRetryManager
 */
public class DefaultMessageRetryManager implements MessageRetryManager{
    private Map<String,MessageSet> cache = new ConcurrentHashMap<>(500);
    private BlockingQueue<MessageSet> queue;
    private int maxRetry;
    private int ttl;

    public DefaultMessageRetryManager(BlockingQueue<MessageSet> queue, int maxRetry, int ttl) {
        this.queue = queue;
        this.maxRetry = maxRetry;
        this.ttl = ttl;

        long period = 5000;
        new Timer().scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                for (Map.Entry<String, MessageSet> entry : cache.entrySet()) {
                    String id = entry.getKey();
                    MessageSet messageSet = entry.getValue();
                    if (now - messageSet.getTimestamp() >= ttl) { // no ack/fail received in ttl
                        fail(id);
                    }
                }
            }
        }, period, period);
    }

    public void ack(String id) {
        cache.remove(id);
    }

    public void fail(String id) {
        MessageSet messageSet = cache.remove(id);
        if (messageSet == null) {
            return;
        }

        if (needRetry(messageSet)) {
            messageSet.setRetries(messageSet.getRetries() + 1);
            messageSet.setTimestamp(0);
            queue.offer(messageSet);
        }
    }

    public void mark(MessageSet messageSet) {
        messageSet.setTimestamp(System.currentTimeMillis());
        cache.put(messageSet.getId(), messageSet);
    }

    public boolean needRetry(MessageSet messageSet) {
        return messageSet.getRetries() < maxRetry;
    }

    // just for testing
    public void setCache(Map<String,MessageSet> cache) {
        this.cache = cache;
    }
}
