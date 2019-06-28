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

package org.apache.storm.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Semaphore;

public class KeyedRoundRobinQueue<V> {
    private final Object lock = new Object();
    private Semaphore size = new Semaphore(0);
    private Map<Object, Queue<V>> queues = new HashMap<>();
    private List<Object> keyOrder = new ArrayList<>();
    private int currIndex = 0;

    public void add(Object key, V val) {
        synchronized (lock) {
            Queue<V> queue = queues.get(key);
            if (queue == null) {
                queue = new LinkedList<>();
                queues.put(key, queue);
                keyOrder.add(key);
            }
            queue.add(val);
        }
        size.release();
    }

    public V take() throws InterruptedException {
        size.acquire();
        synchronized (lock) {
            Object key = keyOrder.get(currIndex);
            Queue<V> queue = queues.get(key);
            V ret = queue.remove();
            if (queue.isEmpty()) {
                keyOrder.remove(currIndex);
                queues.remove(key);
                if (keyOrder.size() == 0) {
                    currIndex = 0;
                } else {
                    currIndex = currIndex % keyOrder.size();
                }
            } else {
                currIndex = (currIndex + 1) % keyOrder.size();
            }
            return ret;
        }
    }
}
