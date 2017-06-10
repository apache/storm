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
package org.apache.storm.messaging.netty;

import org.apache.storm.utils.ObjectReader;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IContext;

public class Context implements IContext {
    @SuppressWarnings("rawtypes")
    private Map<String, Object> topoConf;
    private Map<String, IConnection> connections;
    private NioClientSocketChannelFactory clientChannelFactory;
    
    private HashedWheelTimer clientScheduleService;

    /**
     * initialization per Storm configuration 
     */
    @SuppressWarnings("rawtypes")
    public void prepare(Map<String, Object> topoConf) {
        this.topoConf = topoConf;
        connections = new HashMap<>();

        //each context will have a single client channel factory
        int maxWorkers = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS));
		ThreadFactory bossFactory = new NettyRenameThreadFactory("client" + "-boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory("client" + "-worker");
        if (maxWorkers > 0) {
            clientChannelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                    Executors.newCachedThreadPool(workerFactory), maxWorkers);
        } else {
            clientChannelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                    Executors.newCachedThreadPool(workerFactory));
        }
        
        clientScheduleService = new HashedWheelTimer(new NettyRenameThreadFactory("client-schedule-service"));
    }

    /**
     * establish a server with a binding port
     */
    public synchronized IConnection bind(String storm_id, int port) {
        IConnection server = new Server(topoConf, port);
        connections.put(key(storm_id, server.getPort()), server);
        return server;
    }

    /**
     * establish a connection to a remote server
     */
    public synchronized IConnection connect(String storm_id, String host, int port) {
        IConnection connection = connections.get(key(host,port));
        if(connection !=null)
        {
            return connection;
        }
        IConnection client =  new Client(topoConf, clientChannelFactory, 
                clientScheduleService, host, port, this);
        connections.put(key(host, client.getPort()), client);
        return client;
    }

    synchronized void removeClient(String host, int port) {
        if (connections != null) {
            connections.remove(key(host, port));
        }
    }

    /**
     * terminate this context
     */
    public synchronized void term() {
        clientScheduleService.stop();

        for (IConnection conn : connections.values()) {
            conn.close();
        }

        connections = null;

        //we need to release resources associated with client channel factory
        clientChannelFactory.releaseExternalResources();

    }

    private String key(String host, int port) {
        return String.format("%s:%d", host, port);
    }
}
