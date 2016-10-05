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
package org.apache.storm.jms.spout;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.jms.JmsProvider;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * A Storm <code>Spout</code> implementation that listens to a JMS topic or queue
 * and outputs tuples based on the messages it receives.
 * <p>
 * <code>JmsSpout</code> instances rely on <code>JmsProducer</code> implementations
 * to obtain the JMS <code>ConnectionFactory</code> and <code>Destination</code> objects
 * necessary to connect to a JMS topic/queue.
 * <p>
 * When a <code>JmsSpout</code> receives a JMS message, it delegates to an
 * internal <code>JmsTupleProducer</code> instance to create a Storm tuple from the
 * incoming message.
 * <p>
 * Typically, developers will supply a custom <code>JmsTupleProducer</code> implementation
 * appropriate for the expected message content.
 */
@SuppressWarnings("serial")
public class JmsSpout extends BaseRichSpout implements MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(JmsSpout.class);

    // JMS options
    private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;

    private boolean distributed = true;

    private JmsTupleProducer tupleProducer;

    private JmsProvider jmsProvider;

    private LinkedBlockingQueue<Message> queue;
    private TreeSet<JmsMessageID> toCommit;
    private HashMap<JmsMessageID, Message> pendingMessages;
    private long messageSequence = 0;

    private SpoutOutputCollector collector;

    private transient Connection connection;
    private transient Session session;

    private boolean hasFailures = false;
    public final Serializable recoveryMutex = "RECOVERY_MUTEX";
    private Timer recoveryTimer = null;
    private long recoveryPeriod = -1; // default to disabled

    /**
     * Sets the JMS Session acknowledgement mode for the JMS seesion associated with this spout.
     * <p>
     * Possible values:
     * <ul>
     * <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
     * </ul>
     *
     * @param mode JMS Session Acknowledgement mode
     * @throws IllegalArgumentException if the mode is not recognized.
     */
    public void setJmsAcknowledgeMode(int mode) {
        switch (mode) {
            case Session.AUTO_ACKNOWLEDGE:
            case Session.CLIENT_ACKNOWLEDGE:
            case Session.DUPS_OK_ACKNOWLEDGE:
                break;
            default:
                throw new IllegalArgumentException("Unknown Acknowledge mode: " + mode + " (See javax.jms.Session for valid values)");

        }
        this.jmsAcknowledgeMode = mode;
    }

    /**
     * Returns the JMS Session acknowledgement mode for the JMS seesion associated with this spout.
     *
     * @return
     */
    public int getJmsAcknowledgeMode() {
        return this.jmsAcknowledgeMode;
    }

    /**
     * Set the <code>JmsProvider</code>
     * implementation that this Spout will use to connect to
     * a JMS <code>javax.jms.Desination</code>
     *
     * @param provider
     */
    public void setJmsProvider(JmsProvider provider) {
        this.jmsProvider = provider;
    }

    /**
     * Set the <code>JmsTupleProducer</code>
     * implementation that will convert <code>javax.jms.Message</code>
     * object to <code>org.apache.storm.tuple.Values</code> objects
     * to be emitted.
     *
     * @param producer
     */
    public void setJmsTupleProducer(JmsTupleProducer producer) {
        this.tupleProducer = producer;
    }

    /**
     * <code>javax.jms.MessageListener</code> implementation.
     * <p>
     * Stored the JMS message in an internal queue for processing
     * by the <code>nextTuple()</code> method.
     */
    public void onMessage(Message msg) {
        try {
            LOG.debug("Queuing msg [" + msg.getJMSMessageID() + "]");
        } catch (JMSException e) {
        }
        this.queue.offer(msg);
    }

    /**
     * <code>ISpout</code> implementation.
     * <p>
     * Connects the JMS spout to the configured JMS destination
     * topic/queue.
     */
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        if (this.jmsProvider == null) {
            throw new IllegalStateException("JMS provider has not been set.");
        }
        if (this.tupleProducer == null) {
            throw new IllegalStateException("JMS Tuple Producer has not been set.");
        }
        Integer topologyTimeout = (Integer) conf.get("topology.message.timeout.secs");
        // TODO fine a way to get the default timeout from storm, so we're not hard-coding to 30 seconds (it could change)
        topologyTimeout = topologyTimeout == null ? 30 : topologyTimeout;
        if ((topologyTimeout.intValue() * 1000) > this.recoveryPeriod) {
            LOG.warn("*** WARNING *** : " +
                    "Recovery period (" + this.recoveryPeriod + " ms.) is less then the configured " +
                    "'topology.message.timeout.secs' of " + topologyTimeout +
                    " secs. This could lead to a message replay flood!");
        }
        this.queue = new LinkedBlockingQueue<Message>();
        this.toCommit = new TreeSet<JmsMessageID>();
        this.pendingMessages = new HashMap<JmsMessageID, Message>();
        this.collector = collector;
        try {
            ConnectionFactory cf = this.jmsProvider.connectionFactory();
            Destination dest = this.jmsProvider.destination();
            this.connection = cf.createConnection();
            this.session = connection.createSession(false,
                    this.jmsAcknowledgeMode);
            MessageConsumer consumer = session.createConsumer(dest);
            consumer.setMessageListener(this);
            this.connection.start();
            if (this.isDurableSubscription() && this.recoveryPeriod > 0) {
                this.recoveryTimer = new Timer();
                this.recoveryTimer.scheduleAtFixedRate(new RecoveryTask(), 10, this.recoveryPeriod);
            }

        } catch (Exception e) {
            LOG.warn("Error creating JMS connection.", e);
        }

    }

    public void close() {
        try {
            LOG.debug("Closing JMS connection.");
            this.session.close();
            this.connection.close();
        } catch (JMSException e) {
            LOG.warn("Error closing JMS connection.", e);
        }

    }

    public void nextTuple() {
        Message msg = this.queue.poll();
        if (msg == null) {
            Utils.sleep(50);
        } else {

            LOG.debug("sending tuple: " + msg);
            // get the tuple from the handler
            try {
                Values vals = this.tupleProducer.toTuple(msg);
                // ack if we're not in AUTO_ACKNOWLEDGE mode, or the message requests ACKNOWLEDGE
                LOG.debug("Requested deliveryMode: " + toDeliveryModeString(msg.getJMSDeliveryMode()));
                LOG.debug("Our deliveryMode: " + toDeliveryModeString(this.jmsAcknowledgeMode));
                if (this.isDurableSubscription()) {
                    LOG.debug("Requesting acks.");
                    JmsMessageID messageId = new JmsMessageID(this.messageSequence++, msg.getJMSMessageID());
                    this.collector.emit(vals, messageId);

                    // at this point we successfully emitted. Store
                    // the message and message ID so we can do a
                    // JMS acknowledge later
                    this.pendingMessages.put(messageId, msg);
                    this.toCommit.add(messageId);
                } else {
                    this.collector.emit(vals);
                }
            } catch (JMSException e) {
                LOG.warn("Unable to convert JMS message: " + msg);
            }

        }

    }

    /*
     * Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
     */
    public void ack(Object msgId) {

        Message msg = this.pendingMessages.remove(msgId);
        JmsMessageID oldest = this.toCommit.first();
        if (msgId.equals(oldest)) {
            if (msg != null) {
                try {
                    LOG.debug("Committing...");
                    msg.acknowledge();
                    LOG.debug("JMS Message acked: " + msgId);
                    this.toCommit.remove(msgId);
                } catch (JMSException e) {
                    LOG.warn("Error acknowldging JMS message: " + msgId, e);
                }
            } else {
                LOG.warn("Couldn't acknowledge unknown JMS message ID: " + msgId);
            }
        } else {
            this.toCommit.remove(msgId);
        }

    }

    /*
     * Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
     */
    public void fail(Object msgId) {
        LOG.warn("Message failed: " + msgId);
        this.pendingMessages.clear();
        this.toCommit.clear();
        synchronized (this.recoveryMutex) {
            this.hasFailures = true;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        this.tupleProducer.declareOutputFields(declarer);

    }

    /**
     * Returns <code>true</code> if the spout has received failures
     * from which it has not yet recovered.
     */
    public boolean hasFailures() {
        return this.hasFailures;
    }

    protected void recovered() {
        this.hasFailures = false;
    }

    /**
     * Sets the periodicity of the timer task that
     * checks for failures and recovers the JMS session.
     *
     * @param period
     */
    public void setRecoveryPeriod(long period) {
        this.recoveryPeriod = period;
    }

    public boolean isDistributed() {
        return this.distributed;
    }

    /**
     * Sets the "distributed" mode of this spout.
     * <p>
     * If <code>true</code> multiple instances of this spout <i>may</i> be
     * created across the cluster (depending on the "parallelism_hint" in the topology configuration).
     * <p>
     * Setting this value to <code>false</code> essentially means this spout will run as a singleton
     * within the cluster ("parallelism_hint" will be ignored).
     * <p>
     * In general, this should be set to <code>false</code> if the underlying JMS destination is a
     * topic, and <code>true</code> if it is a JMS queue.
     *
     * @param distributed
     */
    public void setDistributed(boolean distributed) {
        this.distributed = distributed;
    }


    private static final String toDeliveryModeString(int deliveryMode) {
        switch (deliveryMode) {
            case Session.AUTO_ACKNOWLEDGE:
                return "AUTO_ACKNOWLEDGE";
            case Session.CLIENT_ACKNOWLEDGE:
                return "CLIENT_ACKNOWLEDGE";
            case Session.DUPS_OK_ACKNOWLEDGE:
                return "DUPS_OK_ACKNOWLEDGE";
            default:
                return "UNKNOWN";

        }
    }

    protected Session getSession() {
        return this.session;
    }

    private boolean isDurableSubscription() {
        return (this.jmsAcknowledgeMode != Session.AUTO_ACKNOWLEDGE);
    }


    private class RecoveryTask extends TimerTask {
        private final Logger LOG = LoggerFactory.getLogger(RecoveryTask.class);

        public void run() {
            synchronized (JmsSpout.this.recoveryMutex) {
                if (JmsSpout.this.hasFailures()) {
                    try {
                        LOG.info("Recovering from a message failure.");
                        JmsSpout.this.getSession().recover();
                        JmsSpout.this.recovered();
                    } catch (JMSException e) {
                        LOG.warn("Could not recover jms session.", e);
                    }
                }
            }
        }

    }
}
