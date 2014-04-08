package com.alibaba.jstorm.task.execute;

import java.net.URLClassLoader;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.messaging.IConnection;
import backtype.storm.task.IBolt;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.RotatingMap;
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
	private long lastRotate = System.currentTimeMillis();
	private long rotateTime;

	// internal outputCollector is BoltCollector
	private OutputCollector outputCollector;

	public BoltExecutors(IBolt _bolt, TaskTransfer _transfer_fn,
			Map<Integer, DisruptorQueue> innerTaskTransfer, Map storm_conf,
			IConnection _puller, TaskSendTargets _send_fn,
			TaskStatus taskStatus, TopologyContext sysTopologyCxt,
			TopologyContext userTopologyCxt, CommonStatsRolling _task_stats,
			ITaskReportErr _report_error) {

		super(_transfer_fn, storm_conf, _puller, innerTaskTransfer,
				sysTopologyCxt, userTopologyCxt, _task_stats, taskStatus,
				_report_error);

		this.bolt = _bolt;

		// create TimeCacheMap

		this.tuple_start_times = new RotatingMap<Tuple, Long>(
				Acker.TIMEOUT_BUCKET_NUM);
		this.rotateTime = 1000L * message_timeout_secs;

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

		try {
			// do prepare
			WorkerClassLoader.switchThreadContext();
			bolt.prepare(storm_conf, userTopologyCxt, outputCollector);

		} catch (Exception e) {
			error = e;
			LOG.error("bolt prepare error ", e);
			report_error.report(e);
		} finally {
			WorkerClassLoader.restoreThreadContext();
		}

		LOG.info("Successfully create BoltExecutors " + idStr);

	}

	@Override
	public void run() {

		WorkerClassLoader.switchThreadContext();
		while (taskStatus.isShutdown() == false) {
			try {

				long now = System.currentTimeMillis();
				if (now - lastRotate > rotateTime) {
					tuple_start_times.rotate();
					lastRotate = now;
				}

				disruptorRecvQueue.consumeBatchWhenAvailable(this);

			} catch (Exception e) {
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

		Tuple tuple = (Tuple) event;

		task_stats.recv_tuple(tuple.getSourceComponent(),
				tuple.getSourceStreamId());
		
		tuple_start_times.put(tuple, System.currentTimeMillis());

		try {
			bolt.execute(tuple);
		} catch (Exception e) {
			error = e;
			LOG.error("bolt execute error ", e);
			report_error.report(e);
		}
	}

}
