package com.alibaba.jstorm.task.execute;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.timer.RotatingMapTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TickTupleTrigger;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.Metrics;
//import com.alibaba.jstorm.message.zeroMq.IRecvConnection;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

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
	protected DisruptorQueue deserializeQueue;
	protected KryoTupleDeserializer deserializer;
	protected AsyncLoopThread deserializeThread;
	protected JStormTimer  deserializeTimer;

	protected TopologyContext userTopologyCtx;
	protected CommonStatsRolling task_stats;

	protected volatile TaskStatus taskStatus;

	protected int message_timeout_secs = 30;

	protected Throwable error = null;

	protected ITaskReportErr report_error;

	protected DisruptorQueue exeQueue;
	protected Map<Integer, DisruptorQueue> innerTaskTransfer;
	
	

	// protected IntervalCheck intervalCheck = new IntervalCheck();

	public BaseExecutors(TaskTransfer _transfer_fn, Map _storm_conf,
			DisruptorQueue deserializeQueue,
			Map<Integer, DisruptorQueue> innerTaskTransfer,
			TopologyContext topology_context, TopologyContext _user_context,
			CommonStatsRolling _task_stats, TaskStatus taskStatus,
			ITaskReportErr _report_error) {

		this.storm_conf = _storm_conf;
		this.deserializeQueue = deserializeQueue;

		this.userTopologyCtx = _user_context;
		this.task_stats = _task_stats;
		this.taskId = topology_context.getThisTaskId();
		this.innerTaskTransfer = innerTaskTransfer;
		this.component_id = topology_context.getThisComponentId();
		this.idStr = JStormServerUtils.getName(component_id, taskId);

		this.taskStatus = taskStatus;
		this.report_error = _report_error;

		this.deserializer = new KryoTupleDeserializer(storm_conf,
				topology_context);// (KryoTupleDeserializer.

		this.isDebugRecv = ConfigExtension.isTopologyDebugRecvTuple(storm_conf);
		this.isDebug = JStormUtils.parseBoolean(
				storm_conf.get(Config.TOPOLOGY_DEBUG), false);

		message_timeout_secs = JStormUtils.parseInt(
				storm_conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);

		int queue_size = Utils.getInt(
				storm_conf.get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE),
				256);
		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		this.exeQueue = DisruptorQueue.mkInstance(idStr, ProducerType.MULTI,
				queue_size, waitStrategy);
		this.exeQueue.consumerStarted();

		this.registerInnerTransfer(exeQueue);

		deserializeThread = new AsyncLoopThread(new DeserializeRunnable(
				deserializeQueue, exeQueue));
		
		deserializeTimer = Metrics.registerTimer(idStr, MetricDef.DESERIALIZE_TIME, String.valueOf(taskId), Metrics.MetricType.TASK);
		Metrics.registerQueue(idStr, MetricDef.DESERIALIZE_QUEUE, deserializeQueue, String.valueOf(taskId), Metrics.MetricType.TASK);
		Metrics.registerQueue(idStr, MetricDef.EXECUTE_QUEUE, exeQueue, String.valueOf(taskId), Metrics.MetricType.TASK);
		
		RotatingMapTrigger rotatingMapTrigger = new RotatingMapTrigger(storm_conf, idStr + "_rotating", exeQueue);
		rotatingMapTrigger.register();
		
	}

	@Override
	public void run() {
		// this function will be override by SpoutExecutor or BoltExecutor
		throw new RuntimeException("Should implement this function");
	}

	@Override
	public Object getResult() {
		if (taskStatus.isRun()) {
			return 0;
		} else if (taskStatus.isPause()) {
			return 0;
		} else if (taskStatus.isShutdown()) {
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
		if (error == null) {
			return null;
		}
		
		return new Exception(error);
	}

	@Override
	public void shutdown() {
		LOG.info("Shutdown executing thread of " + idStr);
		if (taskStatus.isShutdown() == false) {
			LOG.info("Taskstatus isn't shutdown, but enter shutdown method, Occur exception");
		}
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

	public AsyncLoopThread getDeserlizeThread() {
		return deserializeThread;
	}

	class DeserializeRunnable extends RunnableCallback implements EventHandler {

		DisruptorQueue deserializeQueue;
		DisruptorQueue exeQueue;

		DeserializeRunnable(DisruptorQueue deserializeQueue,
				DisruptorQueue exeQueue) {
			this.deserializeQueue = deserializeQueue;
			this.exeQueue = exeQueue;
		}
		
		@Override
		public String getThreadName() {
			return idStr + "-deserializer";
		}

		protected Tuple deserialize(byte[] ser_msg) {
			deserializeTimer.start();
			try {
				if (ser_msg == null) {
					return null;
				}

				if (ser_msg.length == 0) {
					return null;
				} else if (ser_msg.length == 1) {
					byte newStatus = ser_msg[0];
					LOG.info("Change task status as " + newStatus);
					taskStatus.setStatus(newStatus);

					return null;
				}

				// ser_msg.length > 1
				Tuple tuple = deserializer.deserialize(ser_msg);

				if (isDebugRecv) {

					LOG.info(idStr + " receive " + tuple.toString());
				}

				// recv_tuple_queue.offer(tuple);

				return tuple;

			} catch (Throwable e) {
				if (taskStatus.isShutdown() == false) {
					LOG.error(
							idStr + " recv thread error "
									+ JStormUtils.toPrintableString(ser_msg)
									+ "\n", e);
				}
			}finally {
				deserializeTimer.stop();
			}

			return null;
		}

		@Override
		public void onEvent(Object event, long sequence, boolean endOfBatch)
				throws Exception {
			Tuple tuple = deserialize((byte[]) event);

			if (tuple != null) {
				exeQueue.publish(tuple);
			}
		}

		@Override
		public void run() {
			WorkerClassLoader.switchThreadContext();

			LOG.info("Successfully start recvThread of " + idStr);

			while (taskStatus.isShutdown() == false) {
				try {

					deserializeQueue.consumeBatchWhenAvailable(this);
				} catch (Throwable e) {
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
			LOG.info("Begin to shutdown recvThread of " + idStr);
			return -1;
		}

	}

}
