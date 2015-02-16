package com.alibaba.jstorm.task.execute.spout;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.timer.RotatingMapTrigger;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.Metrics;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.task.execute.BaseExecutors;
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

	protected backtype.storm.spout.ISpout spout;
	protected RotatingMap<Long, TupleInfo> pending;

	protected ISpoutOutputCollector output_collector;

	protected boolean firstTime = true;

	protected JStormTimer nextTupleTimer;
	protected JStormTimer ackerTimer;
	protected TimerRatio emptyCpuCounter;

	protected AsyncLoopThread ackerRunnableThread;
	
	protected boolean         isSpoutFullSleep;

	public SpoutExecutors(backtype.storm.spout.ISpout _spout,
			TaskTransfer _transfer_fn,
			Map<Integer, DisruptorQueue> innerTaskTransfer, Map _storm_conf,
			DisruptorQueue _puller, TaskSendTargets sendTargets,
			TaskStatus taskStatus, TopologyContext topology_context,
			TopologyContext _user_context, CommonStatsRolling _task_stats,
			ITaskReportErr _report_error) {
		super(_transfer_fn, _storm_conf, _puller, innerTaskTransfer,
				topology_context, _user_context, _task_stats, taskStatus,
				_report_error);

		this.spout = _spout;

		this.max_spout_pending = JStormUtils.parseInt(storm_conf
				.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));

		this.nextTupleTimer = Metrics.registerTimer(idStr, MetricDef.EXECUTE_TIME, 
				String.valueOf(taskId), Metrics.MetricType.TASK);
		this.ackerTimer = Metrics.registerTimer(idStr, MetricDef.ACKER_TIME, 
				String.valueOf(taskId), Metrics.MetricType.TASK);
		this.emptyCpuCounter = new TimerRatio();
		Metrics.register(idStr, MetricDef.EMPTY_CPU_RATIO, emptyCpuCounter, 
				String.valueOf(taskId), Metrics.MetricType.TASK);

		isSpoutFullSleep = ConfigExtension.isSpoutPendFullSleep(storm_conf);
		LOG.info("isSpoutFullSleep:" + isSpoutFullSleep);

	}
	

	public void prepare(TaskSendTargets sendTargets, TaskTransfer transferFn,
			TopologyContext topologyContext) {

		
		// collector, in fact it call send_spout_msg
		this.output_collector = new SpoutCollector(taskId, spout, task_stats,
				sendTargets, storm_conf, transferFn, pending, topologyContext,
				exeQueue, report_error);

		try {
			WorkerClassLoader.switchThreadContext();
			this.spout.open(storm_conf, userTopologyCtx,
					new SpoutOutputCollector(output_collector));
		} catch (Throwable e) {
			error = e;
			LOG.error("spout open error ", e);
			report_error.report(e);
		} finally {
			WorkerClassLoader.restoreThreadContext();
		}

		LOG.info("Successfully create SpoutExecutors " + idStr);

	}

	public void nextTuple() {
		if (firstTime == true) {
			
			int delayRun = ConfigExtension.getSpoutDelayRunSeconds(storm_conf);

			// wait other bolt is ready
			JStormUtils.sleepMs(delayRun * 1000);
			
			emptyCpuCounter.init();
			
			if (taskStatus.isRun() == true) {
				spout.activate();
			}else {
				spout.deactivate();
			}

			firstTime = false;
			LOG.info(idStr + " is ready ");
		}

		if (taskStatus.isRun() == false) {
			JStormUtils.sleepMs(1);
			return;
		}

		// if don't need ack, pending map will be always empty
		if (max_spout_pending == null || pending.size() < max_spout_pending) {
			emptyCpuCounter.stop();

			nextTupleTimer.start();
			try {
				spout.nextTuple();
			} catch (Throwable e) {
				error = e;
				LOG.error("spout execute error ", e);
				report_error.report(e);
			} finally {
				nextTupleTimer.stop();
			}

			return;
		} else {
			if (isSpoutFullSleep) {
				JStormUtils.sleepMs(1);
			}
			emptyCpuCounter.start();
			// just return, no sleep
		}
	}

	@Override
	public void run() {

		throw new RuntimeException("Should implement this function");
	}

	/**
	 * Handle acker message
	 * 
	 * @see com.lmax.disruptor.EventHandler#onEvent(java.lang.Object, long,
	 *      boolean)
	 */
	@Override
	public void onEvent(Object event, long sequence, boolean endOfBatch)
			throws Exception {
		ackerTimer.start();
		try {

			if (event == null) {
				return;
			}

			Runnable runnable = null;
			if (event instanceof Tuple) {

				Tuple tuple = (Tuple) event;
				Object id = tuple.getValue(0);
				Object obj = pending.remove((Long) id);

				if (obj == null ) {
					if (isDebug) {
						LOG.info("Pending map no entry:" + id );
					}
					return;
				}

				TupleInfo tupleInfo = (TupleInfo) obj;

				String stream_id = tuple.getSourceStreamId();

				if (stream_id.equals(Acker.ACKER_ACK_STREAM_ID)) {

					runnable = new AckSpoutMsg(spout, tupleInfo, task_stats,
							isDebug);
				} else if (stream_id.equals(Acker.ACKER_FAIL_STREAM_ID)) {
					runnable = new FailSpoutMsg(id, spout, tupleInfo, task_stats,
							isDebug);
				} else {
					LOG.warn("Receive one unknow source Tuple " + idStr);
					return;
				}

				task_stats.recv_tuple(tuple.getSourceComponent(),
						tuple.getSourceStreamId());

			} else if (event instanceof RotatingMapTrigger.Tick) {

				Map<Long, TupleInfo> timeoutMap = pending.rotate();
				for (java.util.Map.Entry<Long, TupleInfo> entry : timeoutMap
						.entrySet()) {
					TupleInfo tupleInfo = entry.getValue();
					FailSpoutMsg fail = new FailSpoutMsg(entry.getKey(), spout,
							(TupleInfo) tupleInfo, task_stats, isDebug);
					fail.run();
				}
				return;
			} else if (event instanceof IAckMsg) {

				runnable = (Runnable) event;
			}  else if (event instanceof Runnable) {

				runnable = (Runnable) event;
			} else {

				LOG.warn("Receive one unknow event " + idStr);
				return;
			}

			runnable.run();

		} catch (Throwable e) {
			if (taskStatus.isShutdown() == false) {
				LOG.info("Unknow excpetion ", e);
				report_error.report(e);
			}
		} finally {
			ackerTimer.stop();
		}
	}

	public AsyncLoopThread getAckerRunnableThread() {
		return ackerRunnableThread;
	}

}
