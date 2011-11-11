package backtype.storm.contrib.jms;

import javax.jms.Message;

import backtype.storm.tuple.Values;
/**
 * JmsMessageProducer implementations are responsible for translating
 * a <code>backtype.storm.tuple.Values</code> instance into a 
 * <code>javax.jms.Message</code> object.
 * <p/>
 * 
 * 
 * @author tgoetz
 *
 */
public interface JmsMessageProducer {

	/**
	 * Translate a <code>backtype.storm.tuple.Values</code> object
	 * to a <code>javax.jms.Message</code object.
	 * @param input
	 * @return
	 */
	public Message toMessage(Values input);
}
