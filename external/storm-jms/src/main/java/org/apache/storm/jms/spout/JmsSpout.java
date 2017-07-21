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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.storm.Config;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * A Storm <code>Spout</code> implementation that listens to a JMS topic or
 * queue and outputs tuples based on the messages it receives.
 *
 * <p><code>JmsSpout</code> instances rely on <code>JmsProducer</code>
 * implementations to obtain the JMS
 * <code>ConnectionFactory</code> and <code>Destination</code> objects necessary
 * to connect to a JMS topic/queue.
 *
 * <p>When a <code>JmsSpout</code> receives a JMS message, it delegates to an
 * internal <code>JmsTupleProducer</code> instance to create a Storm tuple from
 * the incoming message.
 *
 * <p>Typically, developers will supply a custom <code>JmsTupleProducer</code>
 * implementation appropriate for the expected message content.
 */
@SuppressWarnings("serial")
public class JmsSpout extends BaseRichSpout implements MessageListener {

    /** The logger object instance for this class. */
    private static final Logger LOG = LoggerFactory.getLogger(JmsSpout.class);

    /** The logger of the recovery task. */
    private static final Logger RECOVERY_TASK_LOG = LoggerFactory.getLogger(RecoveryTask.class);

    /** Time to sleep between queue polling attempts. */
    private static final int POLL_INTERVAL_MS = 50;

    /**
     * The default value for {@link Config#TOPOLOGY_MESSAGE_TIMEOUT_SECS}.
     */
    private static final int DEFAULT_MESSAGE_TIMEOUT_SECS = 30;

    /** Time to wait before queuing the first recovery task. */
    private static final int RECOVERY_DELAY_MS = 10;

    /**
     * The acknowledgment mode used for this instance.
     *
     * @see Session
     */
    private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;

    /** Indicates whether or not this spout should run as a singleton. */
    private boolean distributed = true;

    /** Used to generate tuples from incoming messages. */
    private JmsTupleProducer tupleProducer;

    /** Encapsulates jms related classes needed to communicate with the mq. */
    private JmsProvider jmsProvider;

    /** Stores incoming messages for later sending. */
    private LinkedBlockingQueue<Message> queue;

    /** Contains all message ids of messages that were not yet acked. */
    private TreeSet<JmsMessageID> toCommit;

    /** Maps between message ids of not-yet acked messages, and the messages. */
    private HashMap<JmsMessageID, Message> pendingMessages;

    /** Counter of handled messages. */
    private long messageSequence = 0;

    /** The collector used to emit tuples. */
    private SpoutOutputCollector collector;

    /** Connection to the jms queue. */
    private transient Connection connection;

    /** The active jms session. */
    private transient Session session;

    /** Indicates whether or not a message failed to be processed. */
    private boolean hasFailures = false;

    /** Used to safely recover failed JMS sessions across instances. */
    private final Serializable recoveryMutex = "RECOVERY_MUTEX";

    /** Schedules recovery tasks periodically. */
    private Timer recoveryTimer = null;

    /** Time to wait between recovery attempts. */
    private long recoveryPeriodMs = -1; // default to disabled

    /**
     * Sets the JMS Session acknowledgement mode for the JMS session.
     *
     * <p>Possible values:
     * <ul>
     * <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
     * </ul>
     *
     * @param mode JMS Session Acknowledgement mode
     * @throws IllegalArgumentException if the mode is not recognized.
     */
    public void setJmsAcknowledgeMode(final int mode) {
        switch (mode) {
            case Session.AUTO_ACKNOWLEDGE:
            case Session.CLIENT_ACKNOWLEDGE:
            case Session.DUPS_OK_ACKNOWLEDGE:
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown Acknowledge mode: " + mode + " (See javax.jms.Session for valid values)");

        }
        this.jmsAcknowledgeMode = mode;
    }

    /**
     * Returns the JMS Session acknowledgement mode for the JMS session
     * associated with this spout. Can be either of:
     * <ul>
     * <li>{@link Session#AUTO_ACKNOWLEDGE}</li>
     * <li>{@link Session#CLIENT_ACKNOWLEDGE}</li>
     * <li>{@link Session#DUPS_OK_ACKNOWLEDGE}</li>
     * <li>{@link Session#SESSION_TRANSACTED}</li>
     * </ul>
     *
     * @return the int value of the acknowledgment mode.
     */
    public int getJmsAcknowledgeMode() {
        return this.jmsAcknowledgeMode;
    }

    /**
     * Set {@link #jmsProvider}.
     *
     * <p>Set the <code>JmsProvider</code>
     * implementation that this Spout will use to connect to
     * a JMS <code>javax.jms.Desination</code>
     *
     * @param provider the provider to use
     */
    public void setJmsProvider(final JmsProvider provider) {
        this.jmsProvider = provider;
    }

    /**
     * Set the <code>JmsTupleProducer</code>
     * implementation that will convert <code>javax.jms.Message</code>
     * object to <code>org.apache.storm.tuple.Values</code> objects
     * to be emitted.
     *
     * @param producer the producer instance to use
     */
    public void setJmsTupleProducer(JmsTupleProducer producer) {
        this.tupleProducer = producer;
    }

    /**
     * <code>javax.jms.MessageListener</code> implementation.
     *
     * <p>Stored the JMS message in an internal queue for processing
     * by the <code>nextTuple()</code> method.
     *
     * @param msg the message to handle
     */
    public void onMessage(Message msg) {
        try {
            LOG.debug("Queuing msg [" + msg.getJMSMessageID() + "]");
        } catch (JMSException ignored) {
            // Do nothing
        }
        this.queue.offer(msg);
    }

    /**
     * <code>ISpout</code> implementation.
     *
     * <p>Connects the JMS spout to the configured JMS destination
     * topic/queue.
     */
    @Override
    public void open(Map<String, Object> conf,
                     TopologyContext context,
                     SpoutOutputCollector collector) {

        if (this.jmsProvider == null) {
            throw new IllegalStateException("JMS provider has not been set.");
        }
        if (this.tupleProducer == null) {
            throw new IllegalStateException("JMS Tuple Producer has not been set.");
        }
        // TODO get the default value from storm instead of hard coding 30 secs
        Long topologyTimeout =
                ((Number) conf.getOrDefault(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, DEFAULT_MESSAGE_TIMEOUT_SECS)).longValue();
        if ((TimeUnit.SECONDS.toMillis(topologyTimeout)) > this.recoveryPeriodMs) {
            LOG.warn("*** WARNING *** : "
                    + "Recovery period (" + this.recoveryPeriodMs + " ms.) is less then the configured "
                    + "'topology.message.timeout.secs' of " + topologyTimeout
                    + " secs. This could lead to a message replay flood!");
        }
        this.queue = new LinkedBlockingQueue<Message>();
        this.toCommit = new TreeSet<JmsMessageID>();
        this.pendingMessages = new HashMap<JmsMessageID, Message>();
        this.collector = collector;
        try {
            ConnectionFactory cf = this.jmsProvider.connectionFactory();
            Destination dest = this.jmsProvider.destination();
            this.connection = cf.createConnection();
            this.session = connection.createSession(false, this.jmsAcknowledgeMode);
            MessageConsumer consumer = session.createConsumer(dest);
            consumer.setMessageListener(this);
            this.connection.start();
            if (this.isDurableSubscription() && this.recoveryPeriodMs > 0) {
                this.recoveryTimer = new Timer();
                this.recoveryTimer.scheduleAtFixedRate(new RecoveryTask(), RECOVERY_DELAY_MS, this.recoveryPeriodMs);
            }

        } catch (Exception e) {
            LOG.warn("Error creating JMS connection.", e);
        }

    }

    /**
     * Close the {@link #session} and {@link #connection}.
     *
     * <p>When overridden, should always call {@code super} to finalize the active connections.
     */
    public void close() {
        try {
            LOG.debug("Closing JMS connection.");
            this.session.close();
            this.connection.close();
        } catch (JMSException e) {
            LOG.warn("Error closing JMS connection.", e);
        }

    }

    /**
     * Generate the next tuple from a message.
     *
     * <p>This method polls the queue that's being filled asynchronously by the
     * jms connection, every {@link #POLL_INTERVAL_MS} seconds. When a message
     * arrives, a {@link Values} (tuple) is generated using
     * {@link #tupleProducer}. It is emitted, and the message is saved to
     * {@link #toCommit} and {@link #pendingMessages} for later handling.
     */
    public void nextTuple() {
        Message msg = this.queue.poll();
        if (msg == null) {
            Utils.sleep(POLL_INTERVAL_MS);
        } else {

            LOG.debug("sending tuple: " + msg);
            // get the tuple from the handler
            try {
                Values vals = this.tupleProducer.toTuple(msg);
                // ack if we're not in AUTO_ACKNOWLEDGE mode,
                // or the message requests ACKNOWLEDGE
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

    /**
     * Ack a successfully handled message by the matching {@link JmsMessageID}.
     *
     * <p>Acking means removing the message from the pending messages
     * collections, and if it was the oldest pending message -
     * ack it to the mq as well, so that it's the only one acked.
     *
     * <p>Will only be called if we're transactional or not AUTO_ACKNOWLEDGE.
     */
    @Override
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

    /**
     * Fail an unsuccessfully handled message by its {@link JmsMessageID}.
     *
     * <p>Failing means dropping all pending messages and queueing a recovery
     * attempt.
     *
     * <p>Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
     */
    @Override
    public void fail(Object msgId) {
        LOG.warn("Message failed: " + msgId);
        this.pendingMessages.clear();
        this.toCommit.clear();
        synchronized (this.recoveryMutex) {
            this.hasFailures = true;
        }
    }

    /**
     * Use the {@link #tupleProducer} to determine which fields are about to be emitted.
     *
     * <p>Note that {@link #nextTuple()} always emits to the default stream, and thus only fields declared
     * for this stream are used.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        this.tupleProducer.declareOutputFields(declarer);

    }

    /**
     * Returns <code>true</code> if the spout has received failures
     * from which it has not yet recovered.
     *
     * @return {@code true} if there were failures, {@code false} otherwise.
     */
    public boolean hasFailures() {
        return this.hasFailures;
    }

    /**
     * Marks a healthy session state.
     */
    protected void recovered() {
        this.hasFailures = false;
    }

    /**
     * Sets the periodicity of the timer task that
     * checks for failures and recovers the JMS session.
     *
     * @param period desired wait period
     */
    public void setRecoveryPeriodMs(long period) {
        this.recoveryPeriodMs = period;
    }

    /**
     * @return {@link #distributed}.
     */
    public boolean isDistributed() {
        return this.distributed;
    }

    /**
     * Sets the "distributed" mode of this spout.
     *
     * <p>If <code>true</code> multiple instances of this spout <i>may</i> be
     * created across the cluster
     * (depending on the "parallelism_hint" in the topology configuration).
     *
     * <p>Setting this value to <code>false</code> essentially means this spout
     * will run as a singleton within the cluster
     * ("parallelism_hint" will be ignored).
     *
     * <p>In general, this should be set to <code>false</code> if the underlying
     * JMS destination is a topic, and <code>true</code> if it is a JMS queue.
     *
     * @param isDistributed {@code true} if should be distributed, {@code false}
     *                      otherwise.
     */
    public void setDistributed(boolean isDistributed) {
        this.distributed = isDistributed;
    }

    /**
     * Translate the {@code int} value of an acknowledgment to a {@code String}.
     *
     * @param deliveryMode the mode to translate.
     * @return its {@code String} explanation (name).
     * @see Session
     */
    private static String toDeliveryModeString(int deliveryMode) {
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

    /**
     * @return The currently active session.
     */
    protected Session getSession() {
        return this.session;
    }

    /**
     * Check if the subscription requires messages to be acked.
     *
     * @return {@code true} if there is a pending messages state, {@code false}
     *         otherwise.
     */
    private boolean isDurableSubscription() {
        return (this.jmsAcknowledgeMode != Session.AUTO_ACKNOWLEDGE);
    }


    /**
     * The periodic task used to try and recover failed sessions.
     */
    private class RecoveryTask extends TimerTask {

        /**
         * Try to recover a failed active session.
         *
         * <p>If there is no active recovery task, and the session is failed,
         * try to recover the session.
         */
        public void run() {
            synchronized (JmsSpout.this.recoveryMutex) {
                if (JmsSpout.this.hasFailures()) {
                    try {
                        RECOVERY_TASK_LOG.info("Recovering from a message failure.");
                        JmsSpout.this.getSession().recover();
                        JmsSpout.this.recovered();
                    } catch (JMSException e) {
                        RECOVERY_TASK_LOG.warn("Could not recover jms session.", e);
                    }
                }
            }
        }

    }
}
