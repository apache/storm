package backtype.storm.contrib.jms;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.Message;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;

/**
 * Interface to define classes that can produce a Storm <code>Values</code> objects
 * from a <code>javax.jms.Message</code> object>.
 * <p/>
 * Implementations are also responsible for declaring the output
 * fields they produce.
 * <p/>
 * If for some reason the implementation can't process a message
 * (for example if it received a <code>javax.jms.ObjectMessage</code>
 * when it was expecting a <code>javax.jms.TextMessage</code> it should
 * return <code>null</code> to indicate to the <code>JmsSpout</code> that
 * the message could not be processed.
 * 
 * @author P. Taylor Goetz
 *
 */
public interface JmsTupleProducer extends Serializable{
	/**
	 * Process a JMS message object to create a Values object.
	 * @param msg - the JMS message
	 * @return the Values tuple, or null if the message couldn't be processed.
	 * @throws JMSException
	 */
	Values toTuple(Message msg) throws JMSException;
	
	/**
	 * Declare the output fields produced by this JmsTupleProducer.
	 * @param declarer The OuputFieldsDeclarer for the spout.
	 */
	void declareOutputFields(OutputFieldsDeclarer declarer);
}
