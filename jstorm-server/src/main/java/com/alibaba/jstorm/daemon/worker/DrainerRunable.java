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
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.Metrics;
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

	private static JStormTimer timer = Metrics.registerTimer(null, 
			MetricDef.DRAINER_TIME, null, Metrics.MetricType.WORKER);

	public DrainerRunable(WorkerData workerData) {
		super(workerData.getSendingQueue(), timer, 
				DrainerRunable.class.getSimpleName(), workerData.getActive());
		
		Metrics.registerQueue(null, MetricDef.DRAINER_QUEUE, queue, null, Metrics.MetricType.WORKER);
	}

	@Override
	public void handleEvent(Object event, boolean endOfBatch)
			throws Exception {

		Pair<IConnection, List<TaskMessage>> pair = (Pair<IConnection, List<TaskMessage>>)event;
		
		pair.getFirst().send(pair.getSecond());
		
	}

}
