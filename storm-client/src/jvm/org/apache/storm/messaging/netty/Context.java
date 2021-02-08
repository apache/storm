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

package org.apache.storm.messaging.netty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.storm.Config;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.IContext;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.shade.io.netty.channel.EventLoopGroup;
import org.apache.storm.shade.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.storm.shade.io.netty.util.HashedWheelTimer;
import org.apache.storm.utils.ObjectReader;

public class Context implements IContext {
    private Map<String, Object> topoConf;
    private List<Server> serverConnections;
    private EventLoopGroup workerEventLoopGroup;
    private HashedWheelTimer clientScheduleService;
    private StormMetricRegistry metricRegistry = null;

    /**
     * initialization per Storm configuration.
     */
    @Override
    public void prepare(Map<String, Object> topoConf) {
        prepare(topoConf, null);
    }

    @Override
    public void prepare(Map<String, Object> topoConf, StormMetricRegistry metricRegistry) {
        this.topoConf = topoConf;
        serverConnections = new ArrayList<>();

        //each context will have a single client channel worker event loop group
        int maxWorkers = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS));
        ThreadFactory workerFactory = new NettyRenameThreadFactory("client" + "-worker");
        // 0 means DEFAULT_EVENT_LOOP_THREADS
        // https://github.com/netty/netty/blob/netty-4.1.24.Final/transport/src/main/java/io/netty/channel/MultithreadEventLoopGroup.java#L40
        this.workerEventLoopGroup = new NioEventLoopGroup(maxWorkers > 0 ? maxWorkers : 0, workerFactory);

        clientScheduleService = new HashedWheelTimer(new NettyRenameThreadFactory("client-schedule-service"));
        this.metricRegistry = metricRegistry;
    }

    /**
     * establish a server with a binding port.
     */
    @Override
    public synchronized IConnection bind(String stormId, int port, IConnectionCallback cb, Supplier<Object> newConnectionResponse) {
        Server server = new Server(topoConf, port, cb, newConnectionResponse);
        serverConnections.add(server);
        return server;
    }

    /**
     * establish a connection to a remote server.
     */
    @Override
    public IConnection connect(String stormId, String host, int port, AtomicBoolean[] remoteBpStatus) {
        return new Client(topoConf, remoteBpStatus, workerEventLoopGroup,
                                        clientScheduleService, host, port, metricRegistry);
    }

    /**
     * terminate this context.
     */
    @Override
    public synchronized void term() {
        clientScheduleService.stop();

        for (Server conn : serverConnections) {
            conn.close();
        }
        serverConnections = null;

        //we need to release resources associated with the worker event loop group
        workerEventLoopGroup.shutdownGracefully().awaitUninterruptibly();

    }
}
