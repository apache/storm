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
package org.apache.storm.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InprocMessaging {
    private static Map<Integer, LinkedBlockingQueue<Object>> _queues = new HashMap<Integer, LinkedBlockingQueue<Object>>();
    private static ConcurrentMap<Integer, AtomicBoolean> _hasReader = new ConcurrentHashMap<>();
    private static int port = 1;
    private static final Logger LOG = LoggerFactory.getLogger(InprocMessaging.class);
    
    public synchronized static int acquireNewPort() {
        int ret = port;
        port++;
        return ret;
    }
    
    public static void sendMessage(int port, Object msg) {
        waitForReader(port);
        getQueue(port).add(msg);
    }
    
    public static void sendMessageNoWait(int port, Object msg) {
        getQueue(port).add(msg);
    }
    
    public static Object takeMessage(int port) throws InterruptedException {
        readerArrived(port);
        return getQueue(port).take();
    }

    public static Object pollMessage(int port) {
        readerArrived(port);
        return  getQueue(port).poll();
    }
    
    private static AtomicBoolean getHasReader(int port) {
        AtomicBoolean ab = _hasReader.get(port);
        if (ab == null) {
            _hasReader.putIfAbsent(port, new AtomicBoolean(false));
            ab = _hasReader.get(port);
        }
        return ab;
    }
    
    public static void waitForReader(int port) {
        AtomicBoolean ab = getHasReader(port);
        long start = Time.currentTimeMillis();
        while (!ab.get()) {
            if (Time.isSimulating()) {
                Time.advanceTime(100);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                //Ignored
            }
            if (Time.currentTimeMillis() - start > 20000) {
                LOG.error("DONE WAITING FOR READER AFTER {} ms", Time.currentTimeMillis() - start);
                break;
            }
        }
    }
    
    private static void readerArrived(int port) {
        getHasReader(port).set(true);
    }
    
    private synchronized static LinkedBlockingQueue<Object> getQueue(int port) {
        if(!_queues.containsKey(port)) {
            _queues.put(port, new LinkedBlockingQueue<Object>());
        }
        return _queues.get(port);
    }

}
