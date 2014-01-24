package com.alibaba.jstorm.task.execute;

import java.net.URLClassLoader;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.WorkerClassLoader;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;
import com.lmax.disruptor.EventHandler;

/**
 * spout executor
 * 
 * All spout actions will be done here
 * 
 * @author yannian/Longda
 * 
 */
public class SpoutExecutors extends BaseExecutors implements EventHandler {
	private static Logger LOG = Logger.getLogger(SpoutExecutors.class);

	protected final Integer max_spout_pending;
	protected final boolean supportRecvThread;

	protected backtype.storm.spout.ISpout spout;
	protected RotatingMap pending;
	private long lastRotate;
	private long rotateTime;

	protected ISpoutOutputCollector output_collector;

	private boolean firstTime = true;

	public SpoutExecutors(backtype.storm.spout.ISpout _spout,
			TaskTransfer _transfer_fn,
			Map<Integer, DisruptorQueue> innerTaskTransfer, Map _storm_conf,
			IConnection _puller, TaskSendTargets sendTargets,
			TaskStatus taskStatus, TopologyContext topology_context,
			TopologyContext _user_context, CommonStatsRolling _task_stats,
			ITaskReportErr _report_error) {
		super(_transfer_fn, _storm_conf, _puller, innerTaskTransfer,
				topology_context, _user_context, _task_stats, taskStatus,
				_report_error);

		this.spout = _spout;

		// sending Tuple's TimeCacheMap
		this.pending = new RotatingMap(Acker.TIMEOUT_BUCKET_NUM,
				new SpoutTimeoutCallBack<Object, Object>(disruptorRecvQueue,
						spout, storm_conf, task_stats));
		this.rotateTime = 1000L * JStormUtils.parseInt(
				storm_conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
		this.lastRotate = System.currentTimeMillis();

		this.max_spout_pending = JStormUtils.parseInt(storm_conf
				.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
		if (max_spout_pending != null && max_spout_pending.intValue() == 1) {
			LOG.info("Do recv/ack in execute thread");
			supportRecvThread = false;
		} else {
			LOG.info("Do recv/ack in extra thread");
			supportRecvThread = true;
		}

		if (supportRecvThread == true) {

			AsyncLoopThread thread = new AsyncLoopThread(new AckerRunnable());
		}

		// collector, in fact it call send_spout_msg
		this.output_collector = new SpoutCollector(taskId, spout, task_stats,
				sendTargets, storm_conf, _transfer_fn, pending,
				topology_context, disruptorRecvQueue, _report_error);

		try {
			WorkerClassLoader.switchThreadContext();
			this.spout.open(storm_conf, userTopologyCtx,
					new SpoutOutputCollector(output_collector));
		} catch (Exception e) {
			error = e;
			LOG.error("spout open error ", e);
			report_error.report(e);
		} finally {
			WorkerClassLoader.restoreThreadContext();
		}

		LOG.info("Successfully create SpoutExecutors " + idStr);
	}

	@Override
	public void run() {

		if (firstTime == true) {
			int delayRun = ConfigExtension.getSpoutDelayRunSeconds(storm_conf);

			// wait other bolt is ready
			JStormUtils.sleepMs(delayRun * 1000);

			firstTime = false;

			LOG.info(idStr + " is ready ");
		}

		if (supportRecvThread == false) {
			executeEvent();
		}

		if (taskStatus.isRun() == false) {
			if (supportRecvThread == true) {
				JStormUtils.sleepMs(10);
			} else {
				JStormUtils.sleepMs(1);
			}
			return;
		}

		// if don't need ack, pending map will be always empty
		if (max_spout_pending == null || pending.size() < max_spout_pending) {

			try {
				WorkerClassLoader.switchThreadContext();
				spout.nextTuple();
			} catch (Exception e) {
				error = e;
				LOG.error("spout execute error ", e);
				report_error.report(e);
			} finally {
				WorkerClassLoader.restoreThreadContext();
			}

			return;
		} else {
			if (supportRecvThread == true) {
				JStormUtils.sleepMs(1);
			} else {
				// if here do sleep, the tps will slow down much
				// JStormUtils.sleepNs(100);

			}
		}

	}

	@Override
	public void onEvent(Object event, long sequence, boolean endOfBatch)
			throws Exception {
		try {

			if (event == null) {
				return;
			}

			Runnable runnable = null;
			if (event instanceof IAckMsg) {

				runnable = (Runnable) event;
			} else if (event instanceof Tuple) {

				Tuple tuple = (Tuple) event;
				Object id = tuple.getValue(0);
				Object obj = pending.remove(id);

				if (obj == null) {
					LOG.warn("Pending map no entry:" + id + ", pending size:"
							+ pending.size());
					return;
				}

				TupleInfo tupleInfo = (TupleInfo) obj;

				String stream_id = tuple.getSourceStreamId();

				if (stream_id.equals(Acker.ACKER_ACK_STREAM_ID)) {

					runnable = new AckSpoutMsg(spout, tupleInfo.getMessageId(),
							isDebug, tupleInfo.getStream(),
							tupleInfo.getTimestamp(), task_stats);
				} else if (stream_id.equals(Acker.ACKER_FAIL_STREAM_ID)) {
					Long time_delta = null;

					runnable = new FailSpoutMsg(spout, tupleInfo, task_stats,
							isDebug);
				}

			} else if (event instanceof Runnable) {

				runnable = (Runnable) event;
			} else {

				LOG.warn("Receive one unknow event " + idStr);
				return;
			}

			WorkerClassLoader.switchThreadContext();
			try {
				runnable.run();
			} finally {
				WorkerClassLoader.restoreThreadContext();
			}

		} catch (Exception e) {
			if (taskStatus.isShutdown() == false) {
				LOG.info("Unknow excpetion ", e);
				report_error.report(e);
			}
		}
	}

	private void executeEvent() {
		try {
			if (supportRecvThread == true) {
				disruptorRecvQueue.consumeBatchWhenAvailable(this);
			} else {
				disruptorRecvQueue.consumeBatch(this);
			}
		} catch (Exception e) {
			if (taskStatus.isShutdown() == false) {
				LOG.error("Actor occur unknow exception ", e);
			}
		}

		long now = System.currentTimeMillis();
		if (now - lastRotate > rotateTime) {
			lastRotate = now;
			// TODO, should do anything to dead??
			Map<Long, TupleInfo> dead = pending.rotate();

		}
	}

	class AckerRunnable extends RunnableCallback {

		@Override
		public void run() {
			LOG.info("Successfully start Spout's acker thread " + idStr);

			while (SpoutExecutors.this.taskStatus.isShutdown() == false) {
				executeEvent();

			}

			LOG.info("Successfully shutdown Spout's acker thread " + idStr);
		}

		public Object getResult() {
			return -1;
		}

	}

}
