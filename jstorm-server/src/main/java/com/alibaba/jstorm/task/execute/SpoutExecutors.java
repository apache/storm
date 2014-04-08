package com.alibaba.jstorm.task.execute;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormUtils;
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
	protected TimeCacheMap<Long, TupleInfo> pending;

	protected ISpoutOutputCollector output_collector;

	private boolean firstTime = true;

	private AsyncLoopThread ackerRunnableThread;

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
		
		int msgTimeout = JStormUtils.parseInt(
				storm_conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
		this.pending = new TimeCacheMap<Long, TupleInfo>(msgTimeout, 
				Acker.TIMEOUT_BUCKET_NUM, 
				new SpoutTimeoutCallBack<Long, TupleInfo>(disruptorRecvQueue,
						spout, storm_conf, task_stats));

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

			ackerRunnableThread = new AsyncLoopThread(new AckerRunnable());
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
				Object obj = pending.remove((Long)id);

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
					runnable = new FailSpoutMsg(spout, tupleInfo, task_stats,
							isDebug);
				} else {
					LOG.warn("Receive one unknow source Tuple " + idStr);
					return;
				}

				task_stats.recv_tuple(tuple.getSourceComponent(),
						tuple.getSourceStreamId());

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

	}

	public AsyncLoopThread getAckerRunnableThread() {
		return ackerRunnableThread;
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
