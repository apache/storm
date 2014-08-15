package backtype.storm.contrib.jms.trident;

import backtype.storm.tuple.Tuple;
import storm.trident.tuple.TridentTuple;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.io.Serializable;

public interface TridentJmsMessageProducer extends Serializable{

	/**
	 * Translate a <code>backtype.storm.tuple.TridentTuple</code> object
	 * to a <code>javax.jms.Message</code object.
	 *
	 * @param session
	 * @param input
	 * @return
	 * @throws javax.jms.JMSException
	 */
	public Message toMessage(Session session, TridentTuple input) throws JMSException;
}
