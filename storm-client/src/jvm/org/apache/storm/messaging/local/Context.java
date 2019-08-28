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

package org.apache.storm.messaging.local;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.IContext;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.messaging.netty.BackPressureStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Context implements IContext {
    private static final Logger LOG = LoggerFactory.getLogger(Context.class);
    private final ConcurrentHashMap<String, LocalServer> registry = new ConcurrentHashMap<>();

    private static String getNodeKey(String nodeId, int port) {
        return nodeId + "-" + port;
    }
    
    private LocalServer createLocalServer(String nodeId, int port, IConnectionCallback cb) {
        String key = getNodeKey(nodeId, port);
        LocalServer ret = new LocalServer(port, cb);
        LocalServer existing = registry.put(key, ret);
        if (existing != null) {
            //Can happen if worker is restarted in the same topology, e.g. due to blob update
            LOG.info("Replacing existing server for key {}", existing, ret, key);
        }
        return ret;
    }

    @Override
    public void prepare(Map<String, Object> topoConf) {
        //NOOP
    }

    @Override
    public IConnection bind(String stormId, int port, IConnectionCallback cb, Supplier<Object> newConnectionResponse) {
        return createLocalServer(stormId, port, cb);
    }

    @Override
    public IConnection connect(String stormId, String host, int port, AtomicBoolean[] remoteBpStatus) {
        return new LocalClient(stormId, port);
    }

    @Override
    public void term() {
        //NOOP
    }

    private class LocalServer implements IConnection {
        final ConcurrentHashMap<Integer, Double> load = new ConcurrentHashMap<>();
        final int port;
        final IConnectionCallback cb;

        LocalServer(int port, IConnectionCallback cb) {
            this.port = port;
            this.cb = cb;
        }
        
        @Override
        public void send(Iterator<TaskMessage> msgs) {
            throw new IllegalArgumentException("SHOULD NOT HAPPEN");
        }

        @Override
        public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
            Map<Integer, Load> ret = new HashMap<>();
            for (Integer task : tasks) {
                Double found = load.get(task);
                if (found != null) {
                    ret.put(task, new Load(true, found, 0));
                }
            }
            return ret;
        }

        @Override
        public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
            load.putAll(taskToLoad);
        }

        @Override
        public void sendBackPressureStatus(BackPressureStatus bpStatus) {
            throw new RuntimeException("Local Server connection should not send BackPressure status");
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public void close() {
            //NOOP
        }
    }

    private class LocalClient implements IConnection {
        //Messages sent before the server registered a callback
        private final LinkedBlockingQueue<TaskMessage> pendingDueToUnregisteredServer;
        private final ScheduledExecutorService pendingFlusher;
        private final int port;
        private final String registryKey;

        LocalClient(String stormId, int port) {
            this.port = port;
            this.registryKey = getNodeKey(stormId, port);
            pendingDueToUnregisteredServer = new LinkedBlockingQueue<>();
            pendingFlusher = Executors.newScheduledThreadPool(1, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable);
                    thread.setName("LocalClientFlusher-" + thread.getId());
                    thread.setDaemon(true);
                    return thread;
                }
            });
            pendingFlusher.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        //Ensure messages are flushed even if no more sends are performed
                        flushPending();
                    } catch (Throwable t) {
                        LOG.error("Uncaught throwable in pending message flusher thread, messages may be lost", t);
                        throw new RuntimeException(t);
                    }
                }
            }, 5, 5, TimeUnit.SECONDS);
        }

        private void flushPending() {
            //Can't cache server in client, server can change when workers restart.
            LocalServer server = registry.get(registryKey);
            if (server != null && !pendingDueToUnregisteredServer.isEmpty()) {
                ArrayList<TaskMessage> ret = new ArrayList<>();
                pendingDueToUnregisteredServer.drainTo(ret);
                server.cb.recv(ret);
            }
        }

        @Override
        public void send(Iterator<TaskMessage> msgs) {
            LocalServer server = registry.get(registryKey);
            if (server != null) {
                flushPending();
                ArrayList<TaskMessage> ret = new ArrayList<>();
                while (msgs.hasNext()) {
                    ret.add(msgs.next());
                }
                server.cb.recv(ret);
            } else {
                while (msgs.hasNext()) {
                    pendingDueToUnregisteredServer.add(msgs.next());
                }
            }
        }

        @Override
        public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
            LocalServer server = registry.get(registryKey);
            if (server != null) {
                return server.getLoad(tasks);
            }
            return Collections.emptyMap();
        }

        @Override
        public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
            LocalServer server = registry.get(registryKey);
            if (server != null) {
                server.sendLoadMetrics(taskToLoad);
            }
        }

        @Override
        public void sendBackPressureStatus(BackPressureStatus bpStatus) {
            throw new RuntimeException("Local Client connection should not send BackPressure status");
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public void close() {
            pendingFlusher.shutdown();
            try {
                pendingFlusher.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while awaiting flusher shutdown", e);
            }
        }
    }
}
