package com.alibaba.jstorm.daemon.worker;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.utils.DisruptorRunable;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.utils.RunCounter;
import com.lmax.disruptor.EventHandler;

//import com.alibaba.jstorm.message.zeroMq.ISendConnection;

/**
 * 
 * Tuple sender
 * 
 * @author yannian
 * 
 */
public class DrainerRunable extends DisruptorRunable{
	private final static Logger LOG = Logger.getLogger(DrainerRunable.class);



	public DrainerRunable(WorkerData workerData) {
		super(workerData.getSendingQueue(), 
				DrainerRunable.class.getSimpleName(), workerData.getActive());
	}

	@Override
	public void handleEvent(Object event, boolean endOfBatch)
			throws Exception {

		Pair<IConnection, List<TaskMessage>> pair = (Pair<IConnection, List<TaskMessage>>)event;
		
		pair.getFirst().send(pair.getSecond());
		
	}

}
