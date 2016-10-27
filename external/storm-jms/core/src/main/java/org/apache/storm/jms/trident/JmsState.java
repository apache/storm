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
package org.apache.storm.jms.trident;

import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.topology.FailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;

import javax.jms.*;
import java.io.Serializable;
import java.lang.IllegalStateException;
import java.util.List;

public class JmsState implements State {

    private static final Logger LOG = LoggerFactory.getLogger(JmsState.class);

    private Options options;
    private Connection connection;
    private Session session;
    private MessageProducer messageProducer;

    protected JmsState(Options options) {
        this.options = options;
    }

    public static class Options implements Serializable {
        private JmsProvider jmsProvider;
        private JmsMessageProducer msgProducer;
        private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;
        private boolean jmsTransactional = true;

        public Options withJmsProvider(JmsProvider provider) {
            this.jmsProvider = provider;
            return this;
        }

        public Options withMessageProducer(JmsMessageProducer msgProducer) {
            this.msgProducer = msgProducer;
            return this;
        }

        public Options withJmsAcknowledgeMode(int jmsAcknowledgeMode) {
            this.jmsAcknowledgeMode = jmsAcknowledgeMode;
            return this;
        }

        public Options withJmsTransactional(boolean jmsTransactional) {
            this.jmsTransactional = jmsTransactional;
            return this;
        }
    }

    protected void prepare() {
        if(this.options.jmsProvider == null || this.options.msgProducer == null){
            throw new IllegalStateException("JMS Provider and MessageProducer not set.");
        }
        LOG.debug("Connecting JMS..");
        try {
            ConnectionFactory cf = this.options.jmsProvider.connectionFactory();
            Destination dest = this.options.jmsProvider.destination();
            this.connection = cf.createConnection();
            this.session = connection.createSession(this.options.jmsTransactional,
                    this.options.jmsAcknowledgeMode);
            this.messageProducer = session.createProducer(dest);

            connection.start();
        } catch (Exception e) {
            LOG.warn("Error creating JMS connection.", e);
        }
    }

    @Override
    public void beginCommit(Long aLong) {
    }

    @Override
    public void commit(Long aLong) {
        LOG.debug("Committing JMS transaction.");
        if(this.options.jmsTransactional) {
            try {
                session.commit();
            } catch(JMSException e){
                LOG.error("JMS Session commit failed.", e);
            }
        }
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) throws JMSException {
        try {
        for(TridentTuple tuple : tuples) {
                Message msg = this.options.msgProducer.toMessage(this.session, tuple);
                if (msg != null) {
                    if (msg.getJMSDestination() != null) {
                        this.messageProducer.send(msg.getJMSDestination(), msg);
                    } else {
                        this.messageProducer.send(msg);
                    }
                }
            }
        } catch (JMSException e) {
            LOG.warn("Failed to send jmd message for a trident batch ", e);
            if(this.options.jmsTransactional) {
                session.rollback();
            }
            throw new FailedException("Failed to write tuples", e);
        }
    }
}
