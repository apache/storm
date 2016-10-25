/*
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
package org.apache.storm.jms.spout;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.activemq.ActiveMQConnectionFactory;

import org.apache.storm.jms.JmsProvider;

public class MockJmsProvider implements JmsProvider {
    private static final long serialVersionUID = 1L;

    private ConnectionFactory connectionFactory = null;
    private Destination destination = null;
    
    public MockJmsProvider() throws NamingException{
        this.connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false"); 
        Context jndiContext = new InitialContext();
        this.destination = (Destination) jndiContext.lookup("dynamicQueues/FOO.BAR");        

    }
    
    /**
     * Provides the JMS <code>ConnectionFactory</code>
     * @return the connection factory
     * @throws Exception
     */
    public ConnectionFactory connectionFactory() throws Exception{
        return this.connectionFactory;
    }

    /**
     * Provides the <code>Destination</code> (topic or queue) from which the
     * <code>JmsSpout</code> will receive messages.
     * @return
     * @throws Exception
     */
    public Destination destination() throws Exception{
        return this.destination;
    }

}
