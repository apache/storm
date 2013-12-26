package backtype.storm.messaging;

public interface IConnection {   
    /**
     * @param flags 0: block, 1: non-block
     * @return
     */
    public byte[] recv(int flags);
    
    /**
     * send a message with taskId and payload
     * @param taskId task ID
     * @param payload
     */
    public void send(int taskId,  byte[] message);
    
    /**
     * close this connection
     */
    public void close();
    
    public boolean isClosed();
}
