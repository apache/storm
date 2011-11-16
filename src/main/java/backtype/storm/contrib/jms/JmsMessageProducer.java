package backtype.storm.contrib.jms;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * JmsMessageProducer implementations are responsible for translating
 * a <code>backtype.storm.tuple.Values</code> instance into a 
 * <code>javax.jms.Message</code> object.
 * <p/>
 * 
 * 
 * @author P. Taylor Goetz
 *
 */
public interface JmsMessageProducer extends Serializable{
	
	/**
	 * Translate a <code>backtype.storm.tuple.Tuple</code> object
	 * to a <code>javax.jms.Message</code object.
	 * 
	 * @param session
	 * @param input
	 * @return
	 * @throws JMSException
	 */
	public Message toMessage(Session session, Tuple input) throws JMSException;
}
