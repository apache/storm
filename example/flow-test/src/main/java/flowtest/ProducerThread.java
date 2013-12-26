package flowtest;

import java.util.List;
import java.util.Queue;

import org.apache.log4j.Logger;



public class ProducerThread implements Runnable{
	
	public static Logger LOG = Logger.getLogger(ProducerThread.class);
	
	private List<Integer> taskIds;
	
	private int hashNum = 0;
	
	private int threadNum = 1;
	
	private Queue<Integer> queue;
	
	private final int messageNum = 1000;

	@Override
	public void run() {
		// TODO Auto-generated method stub
		LOG.info("Producer Thread " + hashNum + " start!");
		for(Integer taskId : taskIds) {
			if(taskId % threadNum != hashNum) {
				continue;
			}
			int i = 0;
			while(i < messageNum) {
				queue.add(taskId);
				i++;
			}
		}
		LOG.info("Producer Thread " + hashNum + " done!");
	}
	
	public ProducerThread(List<Integer> taskIds,
			int threadNum,
			int hashNum, 
			Queue<Integer> queue) {
		this.taskIds = taskIds;
		this.threadNum = threadNum;
		this.hashNum = hashNum;
		this.queue = queue;
	}

}
