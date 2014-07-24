package backtype.storm.messaging;

import java.util.List;

import backtype.storm.utils.DisruptorQueue;

public interface IConnection {
	
	/**
	 * (flags != 1) synchronously 
	 * (flags==1) asynchronously 
	 * 
	 * @param flags
	 * @return
	 */
	public TaskMessage recv(int flags);
	
	/**
	 * In the new design, receive flow is through registerQueue, 
	 * then push message into queue 
	 * 
	 * @param recvQueu
	 */
	public void registerQueue(DisruptorQueue recvQueu);
	public void enqueue(TaskMessage message);
	
	public void send(List<TaskMessage> messages);
	public void send(TaskMessage message);

	/**
	 * close this connection
	 */
	public void close();

	public boolean isClosed();
}
