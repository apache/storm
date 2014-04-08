package com.alibaba.jstorm.daemon.worker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.callback.RunnableCallback;
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
public class DrainerRunable extends RunnableCallback implements EventHandler {
	private final static Logger LOG = Logger.getLogger(DrainerRunable.class);

	private DisruptorQueue transferQueue;
	private ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;
	private ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;
	private AtomicBoolean workerActive;
	private RunCounter drainerCounter = new RunCounter("DrainerRunable",
			DrainerRunable.class);

	public DrainerRunable(WorkerData workerData) {
		this.transferQueue = workerData.getTransferQueue();
		this.nodeportSocket = workerData.getNodeportSocket();
		this.taskNodeport = workerData.getTaskNodeport();
		this.workerActive = workerData.getActive();
	}

	@Override
	public void onEvent(Object event, long sequence, boolean endOfBatch)
			throws Exception {

		if (event == null) {
			return;
		}
		long before = System.currentTimeMillis();
		TaskMessage felem = (TaskMessage) event;

		int taskId = felem.task();
		byte[] tuple = felem.message();

		WorkerSlot nodePort = taskNodeport.get(taskId);
		if (nodePort == null) {
			String errormsg = "can`t not found IConnection to " + taskId;
			LOG.warn("DrainerRunable warn", new Exception(errormsg));
			return;
		}
		IConnection conn = nodeportSocket.get(nodePort);
		if (conn == null) {
			String errormsg = "can`t not found nodePort " + nodePort;
			LOG.warn("DrainerRunable warn", new Exception(errormsg));
			return;
		}

		if (conn.isClosed() == true) {
			// if connection has been closed, just skip the package
			return;
		}

		conn.send(taskId, tuple);

		long after = System.currentTimeMillis();
		drainerCounter.count(after - before);
	}

	@Override
	public void run() {
		transferQueue.consumerStarted();
		while (workerActive.get()) {
			try {

				transferQueue.consumeBatchWhenAvailable(this);

			} catch (Exception e) {
				if (workerActive.get() == true) {
					LOG.error("DrainerRunable send error", e);
					throw new RuntimeException(e);
				}
			}
		}
	}

	@Override
	public Object getResult() {
		if (workerActive.get())
			return 0;
		else
			return -1;
	}

}
