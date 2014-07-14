package backtype.storm.contrib.jms.spout;

import javax.jms.Message;
import java.io.Serializable;
import java.util.TreeSet;

/**
 * Created by tgoetz on 7/14/14.
 */
public class JmsMessageID implements Comparable<JmsMessageID>, Serializable {

    private String jmsID;

    private Long sequence;

//    private Message message;

    public JmsMessageID(long sequence, String jmsID){
        this.jmsID = jmsID;
        this.sequence = sequence;
    }

//    public void setMessage(Message message){
//        this.message = message;
//    }
//
//    public Message getMessage(){
//        return this.message;
//    }

    public String getJmsID(){
        return this.jmsID;
    }

    @Override
    public int compareTo(JmsMessageID jmsMessageID) {
        return (int)(this.sequence - jmsMessageID.sequence);
    }

    @Override
    public int hashCode() {
        return this.sequence.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof JmsMessageID){
            JmsMessageID id = (JmsMessageID)o;
            return this.jmsID.equals(id.jmsID);
        } else {
            return false;
        }
    }

    public String toString(){
        return String.valueOf(this.sequence);
    }


    public static void main(String[] args) {
        TreeSet<JmsMessageID> set = new TreeSet<JmsMessageID>();
        set.add(new JmsMessageID(7, "bar"));
        set.add(new JmsMessageID(1, "barfoo"));

        set.add(new JmsMessageID(10, "foobar"));
        set.add(new JmsMessageID(3, "foo"));

        for(JmsMessageID id : set){
            System.out.println(id);
        }



    }
}
