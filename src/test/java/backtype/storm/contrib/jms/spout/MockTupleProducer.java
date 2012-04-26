package backtype.storm.contrib.jms.spout;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MockTupleProducer implements JmsTupleProducer {
    private static final long serialVersionUID = 1L;

    @Override
    public Values toTuple(Message msg) throws JMSException {
        if (msg instanceof TextMessage) {
            String json = ((TextMessage) msg).getText();
            return new Values(json);
        } else {
            return null;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

}
