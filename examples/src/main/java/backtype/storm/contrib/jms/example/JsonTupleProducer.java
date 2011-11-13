package backtype.storm.contrib.jms.example;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * A simple <code>JmsTupleProducer</code> that expects to receive
 * JMS <code>TextMessage</code> objects with a body in JSON format.
 * <p/>
 * Ouputs a tuple with field name "json" and a string value
 * containing the raw json.
 * <p/>
 * <b>NOTE: </b> Currently this implementation assumes the text is valid
 * JSON and does not attempt to parse or validate it.
 * 
 * @author tgoetz
 *
 */
@SuppressWarnings("serial")
public class JsonTupleProducer implements JmsTupleProducer {

	public Values toTuple(Message msg) throws JMSException {
		if(msg instanceof TextMessage){
			String json = ((TextMessage) msg).getText();
			return new Values(json);
		} else {
			return null;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("json"));
	}

}
