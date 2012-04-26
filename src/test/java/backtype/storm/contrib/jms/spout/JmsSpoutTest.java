package backtype.storm.contrib.jms.spout;

import java.util.HashMap;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Test;
import org.mortbay.log.Log;

import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.spout.SpoutOutputCollector;

public class JmsSpoutTest {
    @Test
    public void testEmit() throws JMSException, Exception{
        JmsSpout spout = new JmsSpout();
        JmsProvider mockProvider = new MockJmsProvider();
        SpoutOutputCollector collector = new SpoutOutputCollector(new MockSpoutOutputCollector());
        spout.setJmsProvider(new MockJmsProvider());
        spout.setJmsTupleProducer(new MockTupleProducer());
        spout.open(new HashMap(), null, collector);
        this.sendMessage(mockProvider.connectionFactory(), mockProvider.destination());
        Thread.sleep(60000);
        spout.nextTuple();
    }

    public void sendMessage(ConnectionFactory connectionFactory, Destination destination) throws JMSException {        
        Session mySess = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = mySess.createProducer(destination);
        TextMessage msg = mySess.createTextMessage();
        msg.setText("Hello World");
        Log.debug("Sending Message: " + msg.getText());
        producer.send(msg);
    }

}
