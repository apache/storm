package com.alibaba.jstorm.daemon.worker;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.utils.DisruptorRunable;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RunCounter;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

//import com.alibaba.jstorm.message.zeroMq.ISendConnection;

/**
 * Message dispatcher
 * 
 * @author yannian/Longda
 * 
 */
public class VirtualPortDispatch extends DisruptorRunable {
	private final static Logger LOG = Logger
			.getLogger(VirtualPortDispatch.class);

	private ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues;
	private IConnection recvConnection;

	public VirtualPortDispatch(WorkerData workerData,
			IConnection recvConnection, DisruptorQueue recvQueue) {
		super(recvQueue, VirtualPortDispatch.class.getSimpleName(), workerData
				.getActive());

		this.recvConnection = recvConnection;
		this.deserializeQueues = workerData.getDeserializeQueues();

	}

	public void cleanup() {
		LOG.info("Begin to shutdown VirtualPortDispatch");
		// don't need send shutdown command to every task
		// due to every task has been shutdown by workerData.active
		// at the same time queue has been fulll
//		byte shutdownCmd[] = { TaskStatus.SHUTDOWN };
//		for (DisruptorQueue queue : deserializeQueues.values()) {
//
//			queue.publish(shutdownCmd);
//		}

		try {
			recvConnection.close();
		}catch(Exception e) {
			
		}
		recvConnection = null;
		LOG.info("Successfully shudown VirtualPortDispatch");
	}

	@Override
	public void handleEvent(Object event, boolean endOfBatch)
			throws Exception {
		TaskMessage message = (TaskMessage) event;

		int task = message.task();

		DisruptorQueue queue = deserializeQueues.get(task);
		if (queue == null) {
			LOG.warn("Received invalid message directed at port " + task
					+ ". Dropping...");
			return;
		}

		queue.publish(message.message());

	}

}
