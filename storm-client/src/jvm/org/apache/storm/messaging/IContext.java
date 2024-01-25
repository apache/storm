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

package org.apache.storm.messaging;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.storm.metrics2.StormMetricRegistry;

/**
 * This interface needs to be implemented for messaging plugin.
 *
 * <p>Messaging plugin is specified via Storm config parameter, storm.messaging.transport.
 *
 * <p>A messaging plugin should have a default constructor and implements IContext interface. Upon construction, we will invoke
 * IContext::prepare(topoConf) to enable context to be configured according to storm configuration.
 */
public interface IContext {
    /**
     * This method is invoked at the startup of messaging plugin.
     *
     * @param topoConf storm configuration
     */
    @Deprecated
    void prepare(Map<String, Object> topoConf);

    /**
     * This method is invoked at the startup of messaging plugin.
     *
     * @param topoConf storm configuration
     * @param metricRegistry storm metric registry
     */
    default void prepare(Map<String, Object> topoConf, StormMetricRegistry metricRegistry) {
        prepare(topoConf);
    }

    /**
     * This method is invoked when a worker is unloading a messaging plugin.
     */
    void term();

    /**
     * This method establishes a server side connection.
     *
     * @param stormId topology ID
     * @param port     port #
     * @param cb The callback to deliver received messages to
     * @param newConnectionResponse Supplier of the initial message to send to new client connections. If authentication
     *                              is required, the message will be sent after authentication is complete.
     * @return server side connection
     */
    IConnection bind(String stormId, int port, IConnectionCallback cb, Supplier<Object> newConnectionResponse);

    /**
     * This method establish a client side connection to a remote server
     * implementation should return a new connection every call.
     *
     * @param stormId       topology ID
     * @param host           remote host
     * @param port           remote port
     * @param remoteBpStatus array of booleans reflecting Back Pressure status of remote tasks.
     * @return client side connection
     */
    IConnection connect(String stormId, String host, int port, AtomicBoolean[] remoteBpStatus);
}
