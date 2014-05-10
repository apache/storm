package com.alibaba.jstorm.task.execute;

import java.net.URLClassLoader;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
//import com.alibaba.jstorm.message.zeroMq.IRecvConnection;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

/**
 * Base executor share between spout and bolt
 * 
 * 
 * @author Longda
 * 
 */
public class BaseExecutors extends RunnableCallback {
	private static Logger LOG = Logger.getLogger(BaseExecutors.class);

	protected final String component_id;
	protected final int taskId;
	protected final boolean isDebugRecv;
	protected final boolean isDebug;
	protected final String idStr;

	protected Map storm_conf;
	// ZMQConnection puller
	protected IConnection puller;

	protected Map<Integer, DisruptorQueue> innerTaskTransfer;

	protected TopologyContext userTopologyCtx;
	protected CommonStatsRolling task_stats;

	protected KryoTupleDeserializer deserializer;

	protected volatile TaskStatus taskStatus;

	protected int message_timeout_secs = 30;

	protected Exception error = null;

	ITaskReportErr report_error;

	protected DisruptorQueue disruptorRecvQueue;

	protected AsyncLoopThread recvThread;

	// protected IntervalCheck intervalCheck = new IntervalCheck();

	public BaseExecutors(TaskTransfer _transfer_fn, Map _storm_conf,
			IConnection _puller,
			Map<Integer, DisruptorQueue> innerTaskTransfer,
			TopologyContext topology_context, TopologyContext _user_context,
			CommonStatsRolling _task_stats, TaskStatus taskStatus,
			ITaskReportErr _report_error) {

		this.storm_conf = _storm_conf;
		this.puller = _puller;

		this.userTopologyCtx = _user_context;
		this.task_stats = _task_stats;
		this.taskId = topology_context.getThisTaskId();
		this.innerTaskTransfer = innerTaskTransfer;
		this.component_id = topology_context.getThisComponentId();
		this.idStr = "ComponentId:" + component_id + ",taskId:" + taskId + " ";

		this.taskStatus = taskStatus;
		this.report_error = _report_error;

		this.deserializer = new KryoTupleDeserializer(storm_conf,
				topology_context);// (KryoTupleDeserializer.

		this.isDebugRecv = ConfigExtension.isTopologyDebugRecvTuple(storm_conf);
		this.isDebug = JStormUtils.parseBoolean(
				storm_conf.get(Config.TOPOLOGY_DEBUG), false);

		message_timeout_secs = JStormUtils.parseInt(
				storm_conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);

		int queue_size = Utils.getInt(storm_conf
				.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE));
		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		this.disruptorRecvQueue = new DisruptorQueue(
				new MultiThreadedClaimStrategy(queue_size), waitStrategy);
		this.disruptorRecvQueue.consumerStarted();

		this.registerInnerTransfer(disruptorRecvQueue);

		recvThread = new AsyncLoopThread(new RecvRunnable(disruptorRecvQueue));
	}

	protected Tuple recv() {
		byte[] ser_msg = null;
		try {
			ser_msg = puller.recv(0);

			if (ser_msg == null) {
				return null;
			}

			if (ser_msg.length == 0) {
				return null;
			} else if (ser_msg.length == 1) {
				byte newStatus = ser_msg[0];
				LOG.info("Change task status as " + newStatus);
				taskStatus.setStatus(newStatus);

				if (newStatus == TaskStatus.SHUTDOWN) {
					puller.close();
				}
				return null;
			}

			// ser_msg.length > 1
			Tuple tuple = deserializer.deserialize(ser_msg);

			if (isDebugRecv) {

				LOG.info(idStr + " receive " + tuple.toString());
			}

			// recv_tuple_queue.offer(tuple);

			return tuple;

		} catch (Exception e) {
			if (taskStatus.isShutdown() == false) {
				LOG.error(
						idStr + " recv thread error "
								+ JStormUtils.toPrintableString(ser_msg) + "\n",
						e);
			}
		}

		return null;
	}

	class RecvRunnable extends RunnableCallback {

		DisruptorQueue recvQueue;

		RecvRunnable(DisruptorQueue recvQueue) {
			this.recvQueue = recvQueue;
		}

		@Override
		public void run() {
			Thread.currentThread().setName(
					component_id + "-" + taskId + "-recvThread");
			WorkerClassLoader.switchThreadContext();

			LOG.info("Successfully start recvThread of " + idStr);

			while (taskStatus.isShutdown() == false) {
				try {
					Tuple tuple = recv();
					if (tuple == null) {
						continue;
					}

					recvQueue.publish(tuple);
				} catch (Exception e) {
					if (taskStatus.isShutdown() == false) {
						LOG.error("Unknow exception ", e);
						report_error.report(e);
					}
				}

			}
			WorkerClassLoader.restoreThreadContext();

			LOG.info("Successfully shutdown recvThread of " + idStr);
		}

		public Object getResult() {
			return -1;
		}

	}

	@Override
	public void run() {
		// this function will be override by SpoutExecutor or BoltExecutor
		LOG.info("BaseExector run");
	}

	@Override
	public Object getResult() {
		if (taskStatus.isRun()) {
			return 0;
		} else if (taskStatus.isPause()) {
			return 0;
		} else if (taskStatus.isShutdown()) {
			LOG.info("Shutdown executing thread of " + idStr);
			this.shutdown();
			return -1;
		} else {
			LOG.info("Unknow TaskStatus, shutdown executing thread of " + idStr);
			this.shutdown();
			return -1;
		}
	}

	@Override
	public Exception error() {
		return error;
	}

	@Override
	public void shutdown() {
		this.unregistorInnerTransfer();
	}

	protected void registerInnerTransfer(DisruptorQueue disruptorQueue) {
		LOG.info("Registor inner transfer for executor thread of " + idStr);
		DisruptorQueue existInnerTransfer = innerTaskTransfer.get(taskId);
		if (existInnerTransfer != null) {
			LOG.info("Exist inner task transfer for executing thread of "
					+ idStr);
			if (existInnerTransfer != disruptorQueue) {
				throw new RuntimeException(
						"Inner task transfer must be only one in executing thread of "
								+ idStr);
			}
		}
		innerTaskTransfer.put(taskId, disruptorQueue);
	}

	protected void unregistorInnerTransfer() {
		LOG.info("Unregistor inner transfer for executor thread of " + idStr);
		innerTaskTransfer.remove(taskId);
	}

	public AsyncLoopThread getRecvThread() {
		return recvThread;
	}

}
