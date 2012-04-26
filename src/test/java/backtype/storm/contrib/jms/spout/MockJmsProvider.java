package backtype.storm.contrib.jms.spout;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.activemq.ActiveMQConnectionFactory;

import backtype.storm.contrib.jms.JmsProvider;

public class MockJmsProvider implements JmsProvider {
    private static final long serialVersionUID = 1L;

    private ConnectionFactory connectionFactory = null;
    private Destination destination = null;
    
    public MockJmsProvider() throws NamingException{
        this.connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false"); 
        Context jndiContext = new InitialContext();
        this.destination = (Destination) jndiContext.lookup("dynamicQueues/FOO.BAR");        

    }
    
    /**
     * Provides the JMS <code>ConnectionFactory</code>
     * @return the connection factory
     * @throws Exception
     */
    public ConnectionFactory connectionFactory() throws Exception{
        return this.connectionFactory;
    }

    /**
     * Provides the <code>Destination</code> (topic or queue) from which the
     * <code>JmsSpout</code> will receive messages.
     * @return
     * @throws Exception
     */
    public Destination destination() throws Exception{
        return this.destination;
    }

}
