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
package org.apache.storm.jms;

import java.io.Serializable;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

/**
 * A <code>JmsProvider</code> object encapsulates the <code>ConnectionFactory</code>
 * and <code>Destination</code> JMS objects the <code>JmsSpout</code> needs to manage
 * a topic/queue connection over the course of it's lifecycle.
 *
 */
public interface JmsProvider extends Serializable {
    /**
     * Provides the JMS <code>ConnectionFactory</code>
     *
     * @return the connection factory
     * @throws Exception
     */
    public ConnectionFactory connectionFactory() throws Exception;

    /**
     * Provides the <code>Destination</code> (topic or queue) from which the
     * <code>JmsSpout</code> will receive messages.
     *
     * @return
     * @throws Exception
     */
    public Destination destination() throws Exception;
}
