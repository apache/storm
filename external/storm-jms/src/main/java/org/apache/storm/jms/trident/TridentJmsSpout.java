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

package org.apache.storm.jms.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import org.apache.storm.Config;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trident implementation of the JmsSpout.
 */
public class TridentJmsSpout implements ITridentSpout<JmsBatch> {

    public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";

    public static final int DEFAULT_BATCH_SIZE = 1000;

    private static final long serialVersionUID = -3469351154693356655L;
    private static int nameIndex = 1;
    private JmsTupleProducer tupleProducer;
    private JmsProvider jmsProvider;
    private int jmsAcknowledgeMode;
    private String name;

    /**
     * Create a TridentJmsSpout with a default name and acknowledge mode of AUTO_ACKNOWLEDGE.
     */
    public TridentJmsSpout() {
        this.name = "JmsSpout_" + (nameIndex++);
        this.jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;
    }

    /**
     * Return a friendly string for the given JMS acknowledge mode, or throw an IllegalArgumentException if
     * the mode is not recognized.
     * <p/>
     * Possible values:
     * <ul>
     * <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
     * </ul>
     * @param acknowledgeMode A valid JMS acknowledge mode
     * @return A friendly string describing the acknowledge mode
     * @throws IllegalArgumentException if the mode is not recognized
     */
    private static String toDeliveryModeString(int acknowledgeMode) {
        switch (acknowledgeMode) {
            case Session.AUTO_ACKNOWLEDGE:
                return "AUTO_ACKNOWLEDGE";
            case Session.CLIENT_ACKNOWLEDGE:
                return "CLIENT_ACKNOWLEDGE";
            case Session.DUPS_OK_ACKNOWLEDGE:
                return "DUPS_OK_ACKNOWLEDGE";
            default:
                throw new IllegalArgumentException(
                    "Unknown JMS Acknowledge mode " + acknowledgeMode + " (See javax.jms.Session for valid values)");
        }
    }

    /**
     * Set the name for this spout, to improve log identification.
     * @param name The name to be used in log messages
     * @return This spout
     */
    public TridentJmsSpout named(String name) {
        this.name = name;
        return this;
    }

    /**
     * Set the <code>JmsProvider</code> implementation that this Spout will use to connect to a JMS
     * <code>javax.jms.Desination</code>.
     */
    public TridentJmsSpout withJmsProvider(JmsProvider provider) {
        this.jmsProvider = provider;
        return this;
    }

    /**
     * Set the <code>JmsTupleProducer</code> implementation that will convert <code>javax.jms.Message</code>
     * object to <code>backtype.storm.tuple.Values</code> objects to be emitted.
     *
     * @return This spout
     */
    public TridentJmsSpout withTupleProducer(JmsTupleProducer tupleProducer) {
        this.tupleProducer = tupleProducer;
        return this;
    }

    /**
     * Set the JMS acknowledge mode for messages being processed by this spout.
     * <p/>
     * Possible values:
     * <ul>
     * <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
     * </ul>
     * @param jmsAcknowledgeMode The chosen acknowledge mode
     * @return This spout
     * @throws IllegalArgumentException if the mode is not recognized
     */
    public TridentJmsSpout withJmsAcknowledgeMode(int jmsAcknowledgeMode) {
        toDeliveryModeString(jmsAcknowledgeMode);
        this.jmsAcknowledgeMode = jmsAcknowledgeMode;
        return this;
    }

    @Override
    public ITridentSpout.BatchCoordinator<JmsBatch> getCoordinator(
        String txStateId, Map<String, Object> conf, TopologyContext context) {
        return new JmsBatchCoordinator(name);
    }

    @Override
    public Emitter<JmsBatch> getEmitter(String txStateId, Map<String, Object> conf, TopologyContext context) {
        return new JmsEmitter(name, jmsProvider, tupleProducer, jmsAcknowledgeMode, conf);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        OutputFieldsGetter fieldGetter = new OutputFieldsGetter();
        tupleProducer.declareOutputFields(fieldGetter);
        StreamInfo streamInfo = fieldGetter.getFieldsDeclaration().get(Utils.DEFAULT_STREAM_ID);
        if (streamInfo == null) {
            throw new IllegalArgumentException("Jms Tuple producer has not declared output fields for the default stream");
        }

        return new Fields(streamInfo.get_output_fields());
    }

    /**
     * The JmsEmitter class listens for incoming messages and stores them in a blocking queue. On each invocation of emit,
     * the queued messages are emitted as a batch.
     *
     */
    private class JmsEmitter implements Emitter<JmsBatch>, MessageListener {

        private final LinkedBlockingQueue<Message> queue;
        private final Connection connection;
        private final Session session;

        private final RotatingMap<Long, List<Message>> batchMessageMap; // Maps transaction Ids to JMS message ids.

        private final long rotateTimeMillis;
        private final int maxBatchSize;
        private final String name;
        private final Logger log = LoggerFactory.getLogger(JmsEmitter.class);
        private long lastRotate;

        JmsEmitter(String name, JmsProvider jmsProvider, JmsTupleProducer tupleProducer, int jmsAcknowledgeMode,
                          Map<String, Object> conf) {
            if (jmsProvider == null) {
                throw new IllegalStateException("JMS provider has not been set.");
            }
            if (tupleProducer == null) {
                throw new IllegalStateException("JMS Tuple Producer has not been set.");
            }

            this.queue = new LinkedBlockingQueue<Message>();
            this.name = name;

            batchMessageMap = new RotatingMap<Long, List<Message>>(3);
            rotateTimeMillis = 1000L * ((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
            lastRotate = System.currentTimeMillis();

            Number batchSize = (Number) conf.get(MAX_BATCH_SIZE_CONF);
            maxBatchSize = batchSize != null ? batchSize.intValue() : DEFAULT_BATCH_SIZE;

            try {
                ConnectionFactory cf = jmsProvider.connectionFactory();
                Destination dest = jmsProvider.destination();
                this.connection = cf.createConnection();
                this.session = connection.createSession(false, jmsAcknowledgeMode);
                MessageConsumer consumer = session.createConsumer(dest);
                consumer.setMessageListener(this);
                this.connection.start();

                log.info(
                    "Created JmsEmitter with max batch size " + maxBatchSize + " rotate time " + rotateTimeMillis
                            + "ms and destination " + dest + " for " + name);

            } catch (Exception e) {
                log.warn("Error creating JMS connection.", e);
                throw new IllegalStateException("Could not create JMS connection for spout ", e);
            }

        }

        @Override
        public void success(TransactionAttempt tx) {

            @SuppressWarnings("unchecked")
            List<Message> messages = (List<Message>) batchMessageMap.remove(tx.getTransactionId());

            if (messages != null) {
                if (!messages.isEmpty()) {
                    log.debug("Success for batch with transaction id " + tx.getTransactionId() + "/" + tx.getAttemptId() + " for " + name);
                }

                for (Message msg : messages) {
                    String messageId = "UnknownId";

                    try {
                        messageId = msg.getJMSMessageID();
                        msg.acknowledge();
                        log.trace("Acknowledged message " + messageId);
                    } catch (JMSException e) {
                        log.warn("Failed to acknowledge message " + messageId, e);
                    }
                }
            } else {
                log.warn("No messages found in batch with transaction id " + tx.getTransactionId() + "/" + tx.getAttemptId());
            }
        }

        /**
         * Fail a batch with the given transaction id. This is called when a batch is timed out, or a new batch with a 
         * matching transaction id is emitted. Note that the current implementation does nothing - i.e. it discards 
         * messages that have been failed.
         * @param transactionId The transaction id of the failed batch
         * @param messages The list of messages to fail.
         */
        private void fail(Long transactionId, List<Message> messages) {
            log.debug("Failure for batch with transaction id " + transactionId + " for " + name);
            if (messages != null) {
                for (Message msg : messages) {
                    try {
                        log.trace("Failed message " + msg.getJMSMessageID());
                    } catch (JMSException e) {
                        log.warn("Could not identify failed message ", e);
                    }
                }
            } else {
                log.warn("Failed batch has no messages with transaction id " + transactionId);
            }
        }

        @Override
        public void close() {
            try {
                log.info("Closing JMS connection.");
                this.session.close();
                this.connection.close();
            } catch (JMSException e) {
                log.warn("Error closing JMS connection.", e);
            }
        }

        @Override
        public void emitBatch(TransactionAttempt tx, JmsBatch coordinatorMeta,
                              TridentCollector collector) {

            long now = System.currentTimeMillis();
            if (now - lastRotate > rotateTimeMillis) {
                Map<Long, List<Message>> failed = batchMessageMap.rotate();
                for (Long id : failed.keySet()) {
                    log.warn("TIMED OUT batch with transaction id " + id + " for " + name);
                    fail(id, failed.get(id));
                }
                lastRotate = now;
            }

            if (batchMessageMap.containsKey(tx.getTransactionId())) {
                log.warn("FAILED duplicate batch with transaction id " + tx.getTransactionId() + "/" + tx.getAttemptId() + " for " + name);
                fail(tx.getTransactionId(), batchMessageMap.get(tx.getTransactionId()));
            }

            List<Message> batchMessages = new ArrayList<Message>();

            for (int index = 0; index < maxBatchSize; index++) {
                Message msg = queue.poll();
                if (msg == null) {
                    Utils.sleep(50); // Back off
                    break;
                }

                try {
                    if (TridentJmsSpout.this.jmsAcknowledgeMode != Session.AUTO_ACKNOWLEDGE) {
                        batchMessages.add(msg);
                    }
                    Values tuple = tupleProducer.toTuple(msg);
                    collector.emit(tuple);
                } catch (JMSException e) {
                    log.warn("Failed to emit message, could not retrieve data for " + name + ": " + e);
                }
            }

            if (!batchMessages.isEmpty()) {
                log.debug("Emitting batch with transaction id " + tx.getTransactionId()
                        + "/" + tx.getAttemptId() + " and size " + batchMessages.size() + " for " + name);
            } else {
                log.trace(
                    "No items to acknowledge for batch with transaction id " + tx.getTransactionId()
                            + "/" + tx.getAttemptId() + " for " + name);
            }
            batchMessageMap.put(tx.getTransactionId(), batchMessages);
        }

        @Override
        public void onMessage(Message msg) {
            try {
                log.trace("Queuing msg [" + msg.getJMSMessageID() + "]");
            } catch (JMSException e) {
                // Nothing here, could not get message id
            }
            this.queue.offer(msg);
        }

    }

    /**
     * Bare implementation of a BatchCoordinator, returning a null JmsBatch object.
     *
     */
    private class JmsBatchCoordinator implements BatchCoordinator<JmsBatch> {

        private final String name;

        private final Logger log = LoggerFactory.getLogger(JmsBatchCoordinator.class);

        JmsBatchCoordinator(String name) {
            this.name = name;
            log.info("Created batch coordinator for " + name);
        }

        @Override
        public JmsBatch initializeTransaction(long txid, JmsBatch prevMetadata, JmsBatch curMetadata) {
            log.debug("Initialise transaction " + txid + " for " + name);
            return null;
        }

        @Override
        public void success(long txid) {
        }

        @Override
        public boolean isReady(long txid) {
            return true;
        }

        @Override
        public void close() {
        }

    }

}

    
