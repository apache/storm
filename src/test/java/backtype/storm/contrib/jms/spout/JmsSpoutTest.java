package backtype.storm.contrib.jms.spout;

import java.util.HashMap;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Assert;
import org.junit.Test;
import org.mortbay.log.Log;

import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.spout.SpoutOutputCollector;

public class JmsSpoutTest {
    @Test
    public void testFailure() throws JMSException, Exception{
        JmsSpout spout = new JmsSpout();
        JmsProvider mockProvider = new MockJmsProvider();
        MockSpoutOutputCollector mockCollector = new MockSpoutOutputCollector();
        SpoutOutputCollector collector = new SpoutOutputCollector(mockCollector);
        spout.setJmsProvider(new MockJmsProvider());
        spout.setJmsTupleProducer(new MockTupleProducer());
        spout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        spout.setRecoveryPeriod(10); // Rapid recovery for testing.
        spout.open(new HashMap<String,String>(), null, collector);
        Message msg = this.sendMessage(mockProvider.connectionFactory(), mockProvider.destination());
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

    public Message sendMessage(ConnectionFactory connectionFactory, Destination destination) throws JMSException {        
        Session mySess = connectionFactory.createConnection().createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer producer = mySess.createProducer(destination);
        TextMessage msg = mySess.createTextMessage();
        msg.setText("Hello World");
        Log.debug("Sending Message: " + msg.getText());
        producer.send(msg);
        return msg;
    }

}
