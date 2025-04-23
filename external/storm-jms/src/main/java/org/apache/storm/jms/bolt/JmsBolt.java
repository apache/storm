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

package org.apache.storm.jms.bolt;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import java.util.Map;
import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JmsBolt receives <code>org.apache.storm.tuple.Tuple</code> objects from a Storm
 * topology and publishes JMS Messages to a destination (topic or queue).
 *
 * <p>To use a JmsBolt in a topology, the following must be supplied:
 * <ol>
 * <li>A <code>JmsProvider</code> implementation.</li>
 * <li>A <code>JmsMessageProducer</code> implementation.</li>
 * </ol>
 * The <code>JmsProvider</code> provides the JMS <code>jakarta.jms.ConnectionFactory</code>
 * and <code>jakarta.jms.Destination</code> objects requied to publish JMS messages.
 *
 * <p>The JmsBolt uses a <code>JmsMessageProducer</code> to translate
 * <code>org.apache.storm.tuple.Tuple</code> objects into
 * <code>jakarta.jms.Message</code> objects for publishing.
 *
 * <p>Both JmsProvider and JmsMessageProducer must be set, or the bolt will
 * fail upon deployment to a cluster.
 *
 * <p>The JmsBolt is typically an endpoint in a topology -- in other words
 * it does not emit any tuples.
 */
public class JmsBolt extends BaseTickTupleAwareRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(JmsBolt.class);

    private boolean autoAck = true;

    // jakarta.jms objects
    private Connection connection;
    private Session session;
    private MessageProducer messageProducer;

    // JMS options
    private boolean jmsTransactional = false;
    private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;


    private JmsProvider jmsProvider;
    private JmsMessageProducer producer;


    private OutputCollector collector;

    /**
     * Set the JmsProvider used to connect to the JMS destination topic/queue.
     */
    public void setJmsProvider(JmsProvider provider) {
        this.jmsProvider = provider;
    }

    /**
     * Set the JmsMessageProducer used to convert tuples
     * into JMS messages.
     */
    public void setJmsMessageProducer(JmsMessageProducer producer) {
        this.producer = producer;
    }

    /**
     * Sets the JMS acknowledgement mode for JMS messages sent
     * by this bolt.
     *
     * <p>Possible values:
     * <ul>
     * <li>jakarta.jms.Session.AUTO_ACKNOWLEDGE</li>
     * <li>jakarta.jms.Session.CLIENT_ACKNOWLEDGE</li>
     * <li>jakarta.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
     * </ul>
     *
     * @param acknowledgeMode (constant defined in jakarta.jms.Session)
     */
    public void setJmsAcknowledgeMode(int acknowledgeMode) {
        this.jmsAcknowledgeMode = acknowledgeMode;
    }

    /**
     * Sets whether or not tuples should be acknowledged by this
     * bolt.
     */
    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }

    /**
     * Consumes a tuple and sends a JMS message.
     *
     * <p>If autoAck is true, the tuple will be acknowledged
     * after the message is sent.
     *
     * <p>If JMS sending fails, the tuple will be failed.
     */
    @Override
    protected void process(Tuple input) {
        // write the tuple to a JMS destination...
        LOG.debug("Tuple received. Sending JMS message.");

        try {
            Message msg = this.producer.toMessage(this.session, input);
            if (msg != null) {
                if (msg.getJMSDestination() != null) {
                    this.messageProducer.send(msg.getJMSDestination(), msg);
                } else {
                    this.messageProducer.send(msg);
                }
            }
            if (this.autoAck) {
                LOG.debug("ACKing tuple: " + input);
                this.collector.ack(input);
            }
        } catch (JMSException e) {
            // failed to send the JMS message, fail the tuple fast
            LOG.warn("Failing tuple: " + input);
            LOG.warn("Exception: ", e);
            this.collector.fail(input);
        }
    }

    /**
     * Releases JMS resources.
     */
    @Override
    public void cleanup() {
        try {
            LOG.debug("Closing JMS connection.");
            this.session.close();
            this.connection.close();
        } catch (JMSException e) {
            LOG.warn("Error closing JMS connection.", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    /**
     * Initializes JMS resources.
     */
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        if (this.jmsProvider == null || this.producer == null) {
            throw new IllegalStateException("JMS Provider and MessageProducer not set.");
        }
        this.collector = collector;
        LOG.debug("Connecting JMS..");
        try {
            ConnectionFactory cf = this.jmsProvider.connectionFactory();
            Destination dest = this.jmsProvider.destination();
            this.connection = cf.createConnection();
            this.session = connection.createSession(this.jmsTransactional,
                                                    this.jmsAcknowledgeMode);
            this.messageProducer = session.createProducer(dest);

            connection.start();
        } catch (Exception e) {
            LOG.warn("Error creating JMS connection.", e);
        }
    }
}
