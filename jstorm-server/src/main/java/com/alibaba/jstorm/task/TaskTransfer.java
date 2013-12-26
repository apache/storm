package com.alibaba.jstorm.task;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.daemon.worker.WorkerHaltRunable;
import com.alibaba.jstorm.stats.Pair;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

/**
 * Sending entrance
 * 
 * Task sending all tuples through this Object
 * 
 * Serialize the Tuple and put the serialized data to the sending queue
 * 
 * @author yannian
 * 
 */
public class TaskTransfer {

	private static Logger                LOG = Logger.getLogger(TaskTransfer.class);
	
	private Map                          storm_conf;
    private DisruptorQueue	             transferQueue;
    private KryoTupleSerializer          serializer;
    private Map<Integer, DisruptorQueue> innerTaskTransfer;
    private DisruptorQueue               serializeQueue;
    private WorkerHaltRunable            halter;
    
    public TaskTransfer(KryoTupleSerializer serializer, Map storm_conf, 
    		DisruptorQueue _transfer_queue, 
    		Map<Integer, DisruptorQueue> innerTaskTransfer,
    		WorkerHaltRunable halter) {
    	
    	this.storm_conf = storm_conf;
        this.transferQueue = _transfer_queue;
        this.serializer = serializer;
        this.innerTaskTransfer = innerTaskTransfer;
        
        int queue_size = Utils.getInt(storm_conf.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE));
        WaitStrategy waitStrategy = (WaitStrategy)Utils.newInstance((String)storm_conf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
        this.serializeQueue = new DisruptorQueue(new SingleThreadedClaimStrategy(queue_size), waitStrategy);
    
        Thread tranferThread = new Thread(new TransferRunnable());
        tranferThread.setName("TaskTransferThread");
        tranferThread.setPriority(Thread.MAX_PRIORITY);
        tranferThread.setDaemon(true);
        tranferThread.start();
    }

    public void transfer(TupleExt tuple) {

        int taskid = tuple.getTargetTaskId();
        
    	DisruptorQueue disrutporQueue = innerTaskTransfer.get(taskid);
    	if (disrutporQueue != null) {
    		disrutporQueue.publish(tuple);
    	} else {
    	    serializeQueue.publish(tuple);
    	}
     
    }
    
    class TransferRunnable implements Runnable, EventHandler {
        
        @Override
        public void run() {
            
            while (true) {
                try {
            		serializeQueue.consumeBatchWhenAvailable(this);
                }catch(Exception e) {
                    LOG.error("Disruptor occur exception ", e);
                    halter.run();
                }
            }
            
        }

		@Override
		public void onEvent(Object event, long sequence, boolean endOfBatch)
				throws Exception {
			
			if (event == null) {
				return;
			}
			
			TupleExt tuple = (TupleExt)event;
			int taskid = tuple.getTargetTaskId();
			byte [] tupleMessage = serializer.serialize(tuple);
    	    TaskMessage taskMessage = new TaskMessage(taskid, tupleMessage);
			transferQueue.publish(taskMessage);
			
		}

    }

}
