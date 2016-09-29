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
package org.apache.storm.messaging.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.IContext;

public class Context implements IContext {
    private static final Logger LOG = LoggerFactory.getLogger(Context.class);

    private static class LocalServer implements IConnection {
        volatile IConnectionCallback _cb;
        final ConcurrentHashMap<Integer, Double> _load = new ConcurrentHashMap<>();

        @Override
        public void registerRecv(IConnectionCallback cb) {
            _cb = cb;
        }

        @Override
        public void send(int taskId,  byte[] payload) {
            throw new IllegalArgumentException("SHOULD NOT HAPPEN");
        }
 
        @Override
        public void send(Iterator<TaskMessage> msgs) {
            throw new IllegalArgumentException("SHOULD NOT HAPPEN");
        }

        @Override
        public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
            Map<Integer, Load> ret = new HashMap<>();
            for (Integer task : tasks) {
                Double found = _load.get(task);
                if (found != null) {
                    ret.put(task, new Load(true, found, 0));
                }
            }
            return ret; 
        }

        @Override
        public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
            _load.putAll(taskToLoad);
        }
 
        @Override
        public void close() {
            //NOOP
        }
    };

    private static class LocalClient implements IConnection {
        private final LocalServer _server;
        //Messages sent before the server registered a callback
        private final LinkedBlockingQueue<TaskMessage> _pendingDueToUnregisteredServer;
        private final ScheduledExecutorService _pendingFlusher;

        public LocalClient(LocalServer server) {
            _server = server;
            _pendingDueToUnregisteredServer = new LinkedBlockingQueue<>();
            _pendingFlusher = Executors.newScheduledThreadPool(1, new ThreadFactory(){
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable);
                    thread.setName("LocalClientFlusher-" + thread.getId());
                    thread.setDaemon(true);
                    return thread;
                }
            });
            _pendingFlusher.scheduleAtFixedRate(new Runnable(){
                @Override
                public void run(){
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

        @Override
        public void registerRecv(IConnectionCallback cb) {
            throw new IllegalArgumentException("SHOULD NOT HAPPEN");
        }
        
        private void flushPending(){
            IConnectionCallback serverCb = _server._cb;
            if (serverCb != null && !_pendingDueToUnregisteredServer.isEmpty()) {
                ArrayList<TaskMessage> ret = new ArrayList<>();
                _pendingDueToUnregisteredServer.drainTo(ret);
                serverCb.recv(ret);
            }
        }
        
        @Override
        public void send(int taskId,  byte[] payload) {
            TaskMessage message = new TaskMessage(taskId, payload);
            IConnectionCallback serverCb = _server._cb;
            if (serverCb != null) {
                flushPending();
                serverCb.recv(Arrays.asList(message));
            } else {
                _pendingDueToUnregisteredServer.add(message);
            }
        }
 
        @Override
        public void send(Iterator<TaskMessage> msgs) {
            IConnectionCallback serverCb = _server._cb;
            if (serverCb != null) {
                flushPending();
                ArrayList<TaskMessage> ret = new ArrayList<>();
                while (msgs.hasNext()) {
                    ret.add(msgs.next());
                }
                serverCb.recv(ret);
            } else {
                while(msgs.hasNext()){
                    _pendingDueToUnregisteredServer.add(msgs.next());
                }
            }
        }

        @Override
        public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
            return _server.getLoad(tasks);
        }

        @Override
        public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
            _server.sendLoadMetrics(taskToLoad);
        }
 
        @Override
        public void close() {
            _pendingFlusher.shutdown();
            try{
                _pendingFlusher.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e){
                throw new RuntimeException("Interrupted while awaiting flusher shutdown", e);
            }
        }
    };

    private static ConcurrentHashMap<String, LocalServer> _registry = new ConcurrentHashMap<>();
    private static LocalServer getLocalServer(String nodeId, int port) {
        String key = nodeId + "-" + port;
        LocalServer ret = _registry.get(key);
        if (ret == null) {
            ret = new LocalServer();
            LocalServer tmp = _registry.putIfAbsent(key, ret);
            if (tmp != null) {
                ret = tmp;
            }
        }
        return ret;
    }
        
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map storm_conf) {
        //NOOP
    }

    @Override
    public IConnection bind(String storm_id, int port) {
        return getLocalServer(storm_id, port);
    }

    @Override
    public IConnection connect(String storm_id, String host, int port) {
        return new LocalClient(getLocalServer(storm_id, port));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void term() {
        //NOOP
    }
}
