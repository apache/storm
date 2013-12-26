package flowtest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class Mspout implements IRichSpout{
	
	public static Logger LOG = Logger.getLogger(Mspout.class);
	
	private SpoutOutputCollector _collector;
	
	private TopologyContext _context;
	
	private LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<Integer>(3000);
	
	private final int producerNum = 3;
	
	private Map<Object, Integer> msgToTask = new HashMap<Object, Integer>();
	
	private Map<Integer, Integer> failToTask = new HashMap<Integer, Integer>();
	
	private volatile boolean state = false;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		_context = context;
		List<Integer> taskIds = _context.getComponentTasks("Mbolt");
		for(int i=0 ; i<producerNum ; i++) {
			Thread thread = new Thread(new ProducerThread(taskIds, producerNum, i, queue));
			thread.start();
		}
		Thread thread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					Thread.sleep(20000);
					while(msgToTask.size() != 0 || !state) {
						Thread.sleep(10000);
						for(java.util.Map.Entry<Integer, Integer> entry : failToTask.entrySet())
							LOG.error("Task " + entry.getKey() + " has fail tuple num " + entry.getValue());
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				LOG.info("All work done");
			}
			
		});
		thread.start();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		try {
			Object msgId = UUID.randomUUID();
			Integer taskId = queue.take();
			msgToTask.put(msgId, taskId);
			state = true;
			_collector.emitDirect(taskId, new Values("aa"), msgId);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		msgToTask.remove(msgId);
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		Integer taskId = msgToTask.remove(msgId);
		Integer failNum = failToTask.get(taskId);
		if(failNum == null) {
			failNum = new Integer(0);
			failToTask.put(taskId, failNum);
		}
		failNum = failNum + 1;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("test"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
