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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.netty.BackPressureStatus;

public interface IConnection extends AutoCloseable {

    /**
     * Send load metrics to all downstream connections.
     *
     * @param taskToLoad a map from the task id to the load for that task.
     */
    void sendLoadMetrics(Map<Integer, Double> taskToLoad);

    /**
     * Sends the back pressure metrics to all downstream connections.
     */
    void sendBackPressureStatus(BackPressureStatus bpStatus);

    /**
     * send batch messages.
     */

    void send(Iterator<TaskMessage> msgs);

    /**
     * Get the current load for the given tasks.
     *
     * @param tasks the tasks to look for.
     * @return a Load for each of the tasks it knows about.
     */
    Map<Integer, Load> getLoad(Collection<Integer> tasks);

    /**
     * Get the port for this connection.
     *
     * @return The port this connection is using
     */
    int getPort();

    /**
     * close this connection.
     */
    @Override
    void close();
}
