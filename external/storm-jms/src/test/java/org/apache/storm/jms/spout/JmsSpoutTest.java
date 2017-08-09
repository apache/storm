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

import org.apache.storm.Config;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.spout.SpoutOutputCollector;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

public class JmsSpoutTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(JmsSpoutTest.class);

    @Test
    public void testFailure() throws JMSException, Exception {
        JmsSpout spout = new JmsSpout();
        JmsProvider mockProvider = new MockJmsProvider();
        MockSpoutOutputCollector mockCollector = new MockSpoutOutputCollector();
        SpoutOutputCollector collector =
                new SpoutOutputCollector(mockCollector);
        spout.setJmsProvider(new MockJmsProvider());
        spout.setJmsTupleProducer(new MockTupleProducer());
        spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        spout.setRecoveryPeriodMs(10); // Rapid recovery for testing.
        spout.open(new HashMap<>(), null, collector);
        ConnectionFactory connectionFactory = mockProvider.connectionFactory();
        Destination destination = mockProvider.destination();
        Message msg = this.sendMessage(connectionFactory, destination);
        Thread.sleep(100);
        spout.nextTuple(); // Pretend to be storm.
        Assert.assertTrue(mockCollector.emitted);

        mockCollector.reset();
        spout.fail(msg.getJMSMessageID()); // Mock failure
        Thread.sleep(5000);
        spout.nextTuple(); // Pretend to be storm.
        Thread.sleep(5000);
        Assert.assertTrue(mockCollector.emitted); // Should have been re-emitted
    }

    @Test
    public void testSerializability() throws IOException {
        JmsSpout spout = new JmsSpout();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(spout);
        oos.close();
        Assert.assertTrue(out.toByteArray().length > 0);
    }

    /**
     * Make sure that {@link JmsSpout#open} returns correctly regardless of
     * the type of {@link Number} that is the value of
     * {@link Config#TOPOLOGY_MESSAGE_TIMEOUT_SECS}.
     */
    @Test
    public void testOpenWorksMultipleTypesOfNumberObjects() throws Exception {
        JmsSpout spout = new JmsSpout();
        spout.setJmsProvider(new MockJmsProvider());
        spout.setJmsTupleProducer(new MockTupleProducer());
        Map<String, Object> configuration = new HashMap<String, Object>();
        MockSpoutOutputCollector delegateCollector =
                new MockSpoutOutputCollector();
        SpoutOutputCollector collector =
                new SpoutOutputCollector(delegateCollector);

        // Test with long value
        configuration.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 1000L);
        spout.open(configuration, null, collector);

        // Test with integer value
        configuration.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 1000);
        spout.open(configuration, null, collector);
    }

    public Message sendMessage(ConnectionFactory connectionFactory,
                               Destination destination) throws JMSException {

        Session mySess = connectionFactory.createConnection().createSession(
                false,
                Session.CLIENT_ACKNOWLEDGE);
        MessageProducer producer = mySess.createProducer(destination);
        TextMessage msg = mySess.createTextMessage();
        msg.setText("Hello World");
        LOG.info("Sending Message: {}", msg.getText());
        producer.send(msg);
        return msg;
    }

}
