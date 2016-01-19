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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import org.apache.storm.Config;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IContext;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

public class Context implements IContext {
    @SuppressWarnings("rawtypes")
    private Map storm_conf;
    private Map<String, IConnection> connections;

    private EventLoopGroup workerEventLoopGroup;

    private HashedWheelTimer clientScheduleService;

    /**
     * initialization per Storm configuration 
     */
    @SuppressWarnings("rawtypes")
    public void prepare(Map storm_conf) {
        this.storm_conf = storm_conf;
        connections = new HashMap<>();

        //each context will have a single client channel workerEventLoopGroup
        int maxWorkers = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS));
        ThreadFactory workerFactory = new NettyRenameThreadFactory("client" + "-worker");

        if (maxWorkers > 0) {
            workerEventLoopGroup = new NioEventLoopGroup(maxWorkers, workerFactory);
        } else {
            // 0 means DEFAULT_EVENT_LOOP_THREADS
            workerEventLoopGroup = new NioEventLoopGroup(0, workerFactory);
        }

        clientScheduleService = new HashedWheelTimer(new NettyRenameThreadFactory("client-schedule-service"));
    }

    /**
     * establish a server with a binding port
     */
    public synchronized IConnection bind(String storm_id, int port) {
        IConnection server = new Server(storm_conf, port);
        connections.put(key(storm_id, port), server);
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
        IConnection client =  new Client(storm_conf, workerEventLoopGroup,
                clientScheduleService, host, port, this);
        connections.put(key(host, port), client);
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

        //we need to release resources associated
        workerEventLoopGroup.shutdownGracefully();
    }

    private String key(String host, int port) {
        return String.format("%s:%d", host, port);
    }
}
