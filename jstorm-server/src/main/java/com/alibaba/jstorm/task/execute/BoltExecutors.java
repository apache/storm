package com.alibaba.jstorm.task.execute;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.IBolt;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.daemon.worker.timer.RotatingMapTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TickTupleTrigger;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.Metrics;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;
import com.alibaba.jstorm.utils.TimeUtils;
import com.lmax.disruptor.EventHandler;

/**
 * 
 * BoltExecutor
 * 
 * @author yannian/Longda
 * 
 */
public class BoltExecutors extends BaseExecutors implements EventHandler {
	private static Logger LOG = Logger.getLogger(BoltExecutors.class);

	protected IBolt bolt;

	protected RotatingMap<Tuple, Long> tuple_start_times;

	private int ackerNum = 0;

	// internal outputCollector is BoltCollector
	private OutputCollector outputCollector;

	private JStormTimer boltExeTimer;

	public BoltExecutors(IBolt _bolt, TaskTransfer _transfer_fn,
			Map<Integer, DisruptorQueue> innerTaskTransfer, Map storm_conf,
			DisruptorQueue deserializeQueue, TaskSendTargets _send_fn,
			TaskStatus taskStatus, TopologyContext sysTopologyCxt,
			TopologyContext userTopologyCxt, CommonStatsRolling _task_stats,
			ITaskReportErr _report_error) {

		super(_transfer_fn, storm_conf, deserializeQueue, innerTaskTransfer,
				sysTopologyCxt, userTopologyCxt, _task_stats, taskStatus,
				_report_error);

		this.bolt = _bolt;

		// create TimeCacheMap

		this.tuple_start_times = new RotatingMap<Tuple, Long>(
				Acker.TIMEOUT_BUCKET_NUM);

		this.ackerNum = JStormUtils.parseInt(storm_conf
				.get(Config.TOPOLOGY_ACKER_EXECUTORS));

		// don't use TimeoutQueue for recv_tuple_queue,
		// then other place should check the queue size
		// TimeCacheQueue.DefaultExpiredCallback<Tuple> logExpireCb = new
		// TimeCacheQueue.DefaultExpiredCallback<Tuple>(
		// idStr);
		// this.recv_tuple_queue = new
		// TimeCacheQueue<Tuple>(message_timeout_secs,
		// TimeCacheQueue.DEFAULT_NUM_BUCKETS, logExpireCb);

		// create BoltCollector
		IOutputCollector output_collector = new BoltCollector(
				message_timeout_secs, _report_error, _send_fn, storm_conf,
				_transfer_fn, sysTopologyCxt, taskId, tuple_start_times,
				_task_stats);

		outputCollector = new OutputCollector(output_collector);

		boltExeTimer = Metrics.registerTimer(idStr, MetricDef.EXECUTE_TIME, 
				String.valueOf(taskId), Metrics.MetricType.TASK);
		
		Object tickFrequence = storm_conf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
		if (tickFrequence != null) {
			Integer frequence = JStormUtils.parseInt(tickFrequence);
			TickTupleTrigger tickTupleTrigger = new TickTupleTrigger(
					sysTopologyCxt, frequence, 
					idStr + Constants.SYSTEM_TICK_STREAM_ID, exeQueue);
			tickTupleTrigger.register();
		}

		try {
			// do prepare
			WorkerClassLoader.switchThreadContext();
			
//			Method method = IBolt.class.getMethod("prepare", new Class[] {Map.class, TopologyContext.class,
//					OutputCollector.class});
//			method.invoke(bolt, new Object[] {storm_conf, userTopologyCxt, outputCollector});
			bolt.prepare(storm_conf, userTopologyCtx, outputCollector);

		} catch (Throwable e) {
			error = e;
			LOG.error("bolt prepare error ", e);
			report_error.report(e);
		} finally {
			WorkerClassLoader.restoreThreadContext();
		}

		LOG.info("Successfully create BoltExecutors " + idStr);

	}
	
	@Override
	public String getThreadName() {
		return idStr + "-" + BoltExecutors.class.getSimpleName();
	}

	@Override
	public void run() {

		WorkerClassLoader.switchThreadContext();
		while (taskStatus.isShutdown() == false) {
			try {

				exeQueue.consumeBatchWhenAvailable(this);

			} catch (Throwable e) {
				if (taskStatus.isShutdown() == false) {
					LOG.error(idStr + " bolt exeutor  error", e);
				}
			}
		}
		WorkerClassLoader.restoreThreadContext();

	}

	@Override
	public void onEvent(Object event, long sequence, boolean endOfBatch)
			throws Exception {

		if (event == null) {
			return;
		}

		boltExeTimer.start();

		try {

			if (event instanceof RotatingMapTrigger.Tick) {
				// don't check the timetick name to improve performance
				
				Map<Tuple, Long> timeoutMap = tuple_start_times.rotate();

				if (ackerNum > 0) {
					// only when acker is enable
					for (Entry<Tuple, Long> entry : timeoutMap.entrySet()) {
						Tuple input = entry.getKey();
						task_stats.bolt_failed_tuple(
								input.getSourceComponent(),
								input.getSourceStreamId());
					}
				}

				return;
			}

			Tuple tuple = (Tuple) event;

			task_stats.recv_tuple(tuple.getSourceComponent(),
					tuple.getSourceStreamId());

			tuple_start_times.put(tuple, System.currentTimeMillis());

			try {
				bolt.execute(tuple);
			} catch (Throwable e) {
				error = e;
				LOG.error("bolt execute error ", e);
				report_error.report(e);
			}

			if (ackerNum == 0) {
				// only when acker is disable
				// get tuple process latency
				Long start_time = (Long) tuple_start_times.remove(tuple);
				if (start_time != null) {
					Long delta = TimeUtils.time_delta_ms(start_time);
					task_stats.bolt_acked_tuple(tuple.getSourceComponent(),
							tuple.getSourceStreamId(), delta);
				}
			}
		} finally {
			boltExeTimer.stop();
		}
	}

}
