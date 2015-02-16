package com.alibaba.jstorm.task.execute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;

import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.Metrics;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * bolt output interface, do emit/ack/fail
 * 
 * @author yannian/Longda
 * 
 */
public class BoltCollector implements IOutputCollector {
	private static Logger LOG = Logger.getLogger(BoltCollector.class);

	private ITaskReportErr reportError;
	private TaskSendTargets sendTargets;
	private TaskTransfer taskTransfer;
	private TopologyContext topologyContext;
	private Integer task_id;
	// private TimeCacheMap<Tuple, Long> tuple_start_times;
	private RotatingMap<Tuple, Long> tuple_start_times;
	private CommonStatsRolling task_stats;
	// private TimeCacheMap<Tuple, Long> pending_acks;
	private RotatingMap<Tuple, Long> pending_acks;
	private long lastRotate = System.currentTimeMillis();
	private long rotateTime;

	private Map storm_conf;
	private Integer ackerNum;
	private JStormTimer   timer;
	private Random  random;

	public BoltCollector(int message_timeout_secs, ITaskReportErr report_error,
			TaskSendTargets _send_fn, Map _storm_conf,
			TaskTransfer _transfer_fn, TopologyContext _topology_context,
			Integer task_id, RotatingMap<Tuple, Long> tuple_start_times,
			CommonStatsRolling _task_stats) {

		this.rotateTime = 1000L * message_timeout_secs/(Acker.TIMEOUT_BUCKET_NUM - 1);
		this.reportError = report_error;
		this.sendTargets = _send_fn;
		this.storm_conf = _storm_conf;
		this.taskTransfer = _transfer_fn;
		this.topologyContext = _topology_context;
		this.task_id = task_id;
		this.task_stats = _task_stats;

		this.pending_acks = new RotatingMap<Tuple, Long>(
				Acker.TIMEOUT_BUCKET_NUM);
		// this.pending_acks = new TimeCacheMap<Tuple,
		// Long>(message_timeout_secs,
		// Acker.TIMEOUT_BUCKET_NUM);
		this.tuple_start_times = tuple_start_times;

		this.ackerNum = JStormUtils.parseInt(storm_conf
				.get(Config.TOPOLOGY_ACKER_EXECUTORS));
		
		String componentId = topologyContext.getThisComponentId();
		timer = Metrics.registerTimer(JStormServerUtils.getName(componentId, task_id), 
				MetricDef.EMIT_TIME, String.valueOf(task_id), Metrics.MetricType.TASK);
		random = new Random();
		random.setSeed(System.currentTimeMillis());
	}

	@Override
	public List<Integer> emit(String streamId, Collection<Tuple> anchors,
			List<Object> tuple) {
		return boltEmit(streamId, anchors, tuple, null);
	}

	@Override
	public void emitDirect(int taskId, String streamId,
			Collection<Tuple> anchors, List<Object> tuple) {
		boltEmit(streamId, anchors, tuple, taskId);
	}

	private List<Integer> boltEmit(String out_stream_id,
			Collection<Tuple> anchors, List<Object> values, Integer out_task_id) {
		timer.start();
		try {
			java.util.List<Integer> out_tasks = null;
			if (out_task_id != null) {
				out_tasks = sendTargets.get(out_task_id, out_stream_id, values);
			} else {
				out_tasks = sendTargets.get(out_stream_id, values);
			}

			for (Integer t : out_tasks) {
				Map<Long, Long> anchors_to_ids = new HashMap<Long, Long>();
				if (anchors != null) {
					for (Tuple a : anchors) {
						//Long edge_id = MessageId.generateId();
						Long edge_id = MessageId.generateId(random);
						long now = System.currentTimeMillis();
						if (now - lastRotate > rotateTime) {
							pending_acks.rotate();
							lastRotate = now;
						}
						put_xor(pending_acks, a, edge_id);
						for (Long root_id : a.getMessageId().getAnchorsToIds()
								.keySet()) {
							put_xor(anchors_to_ids, root_id, edge_id);
						}
					}
				}
				MessageId msgid = MessageId.makeId(anchors_to_ids);
				TupleImplExt tupleExt = new TupleImplExt(topologyContext,
						values, task_id, out_stream_id, msgid);
				tupleExt.setTargetTaskId(t);

				taskTransfer.transfer(tupleExt);

			}
			return out_tasks;
		} catch (Exception e) {
			LOG.error("bolt emit", e);
		}finally {
			timer.stop();
		}
		return new ArrayList<Integer>();
	}

	@Override
	public void ack(Tuple input) {

		if (ackerNum > 0) {

			Long ack_val = Long.valueOf(0);
			Object pend_val = pending_acks.remove(input);
			if (pend_val != null) {
				ack_val = (Long) (pend_val);
			}

			for (Entry<Long, Long> e : input.getMessageId().getAnchorsToIds()
					.entrySet()) {

				UnanchoredSend.send(
						topologyContext,
						sendTargets,
						taskTransfer,
						Acker.ACKER_ACK_STREAM_ID,
						JStormUtils.mk_list((Object) e.getKey(),
								JStormUtils.bit_xor(e.getValue(), ack_val)));
			}
		}

		Long delta = tuple_time_delta(tuple_start_times, input);
		if (delta != null) {
			task_stats.bolt_acked_tuple(input.getSourceComponent(),
					input.getSourceStreamId(), delta);
		}
	}

	@Override
	public void fail(Tuple input) {
		// if ackerNum == 0, we can just return
		if (ackerNum > 0) {
			pending_acks.remove(input);
			for (Entry<Long, Long> e : input.getMessageId().getAnchorsToIds()
					.entrySet()) {
				UnanchoredSend.send(topologyContext, sendTargets, taskTransfer,
						Acker.ACKER_FAIL_STREAM_ID,
						JStormUtils.mk_list((Object) e.getKey()));
			}
		}

		task_stats.bolt_failed_tuple(input.getSourceComponent(),
				input.getSourceStreamId());

	}

	@Override
	public void reportError(Throwable error) {
		reportError.report(error);
	}

	// Utility functions, just used here
	public static Long tuple_time_delta(RotatingMap<Tuple, Long> start_times,
			Tuple tuple) {
		Long start_time = (Long) start_times.remove(tuple);
		if (start_time != null) {
			return TimeUtils.time_delta_ms(start_time);
		}
		return null;
	}

	public static void put_xor(RotatingMap<Tuple, Long> pending, Tuple key,
			Long id) {
		// synchronized (pending) {
		Long curr = pending.get(key);
		if (curr == null) {
			curr = Long.valueOf(0);
		}
		pending.put(key, JStormUtils.bit_xor(curr, id));
		// }
	}

	public static void put_xor(Map<Long, Long> pending, Long key, Long id) {
		// synchronized (pending) {
		Long curr = pending.get(key);
		if (curr == null) {
			curr = Long.valueOf(0);
		}
		pending.put(key, JStormUtils.bit_xor(curr, id));
		// }
	}

}
