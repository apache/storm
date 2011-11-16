package backtype.storm.contrib.jms;

import java.io.Serializable;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
/**
 * A <code>JmsProvider</code> object encapsulates the <code>ConnectionFactory</code>
 * and <code>Destination</code> JMS objects the <code>JmsSpout</code> needs to manage
 * a topic/queue connection over the course of it's lifecycle.
 * 
 * @author P. Taylor Goetz
 *
 */
public interface JmsProvider extends Serializable{
	/**
	 * Provides the JMS <code>ConnectionFactory</code>
	 * @return the connection factory
	 * @throws Exception
	 */
	public ConnectionFactory connectionFactory() throws Exception;

	/**
	 * Provides the <code>Destination</code> (topic or queue) from which the
	 * <code>JmsSpout</code> will receive messages.
	 * @return
	 * @throws Exception
	 */
	public Destination destination() throws Exception;
}
