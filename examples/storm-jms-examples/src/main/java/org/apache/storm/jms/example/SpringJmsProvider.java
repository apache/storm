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

package org.apache.storm.jms.example;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

import org.apache.storm.jms.JmsProvider;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * A <code>JmsProvider</code> that uses the spring framework
 * to obtain a JMS <code>ConnectionFactory</code> and 
 * <code>Desitnation</code> objects.
 *
 * <p>The constructor takes three arguments:
 * <ol>
 * <li>A string pointing to the the spring application context file contining the JMS configuration
 * (must be on the classpath)
 * </li>
 * <li>The name of the connection factory bean</li>
 * <li>The name of the destination bean</li>
 * </ol></p>
 */
@SuppressWarnings("serial")
public class SpringJmsProvider implements JmsProvider {
    private ConnectionFactory connectionFactory;
    private Destination destination;

    /**
     * Constructs a <code>SpringJmsProvider</code> object given the name of a
     * classpath resource (the spring application context file), and the bean
     * names of a JMS connection factory and destination.
     *
     * @param appContextClasspathResource - the spring configuration file (classpath resource)
     * @param connectionFactoryBean - the JMS connection factory bean name
     * @param destinationBean - the JMS destination bean name
     */
    public SpringJmsProvider(String appContextClasspathResource, String connectionFactoryBean, String destinationBean) {
        ApplicationContext context = new ClassPathXmlApplicationContext(appContextClasspathResource);
        this.connectionFactory = (ConnectionFactory) context.getBean(connectionFactoryBean);
        this.destination = (Destination) context.getBean(destinationBean);
    }

    @Override
    public ConnectionFactory connectionFactory() throws Exception {
        return this.connectionFactory;
    }

    @Override
    public Destination destination() throws Exception {
        return this.destination;
    }

}
