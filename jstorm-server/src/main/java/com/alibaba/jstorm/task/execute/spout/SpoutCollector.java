package com.alibaba.jstorm.task.execute.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.Metrics;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeOutMap;

/**
 * spout collector, sending tuple through this Object
 * 
 * @author yannian/Longda
 * 
 */
public class SpoutCollector implements ISpoutOutputCollector {
	private static Logger LOG = Logger.getLogger(SpoutCollector.class);

	private TaskSendTargets sendTargets;
	private Map storm_conf;
	private TaskTransfer transfer_fn;
	// private TimeCacheMap pending;
	private TimeOutMap<Long, TupleInfo> pending;
	// topology_context is system topology context
	private TopologyContext topology_context;

	private DisruptorQueue disruptorAckerQueue;
	private CommonStatsRolling task_stats;
	private backtype.storm.spout.ISpout spout;
	private ITaskReportErr report_error;

	private Integer task_id;
	private Integer ackerNum;
	private boolean isDebug = false;

	private JStormTimer emitTotalTimer;
	Random random;

	public SpoutCollector(Integer task_id, backtype.storm.spout.ISpout spout,
			CommonStatsRolling task_stats, TaskSendTargets sendTargets,
			Map _storm_conf, TaskTransfer _transfer_fn,
			TimeOutMap<Long, TupleInfo> pending,
			TopologyContext topology_context,
			DisruptorQueue disruptorAckerQueue, ITaskReportErr _report_error) {
		this.sendTargets = sendTargets;
		this.storm_conf = _storm_conf;
		this.transfer_fn = _transfer_fn;
		this.pending = pending;
		this.topology_context = topology_context;

		this.disruptorAckerQueue = disruptorAckerQueue;

		this.task_stats = task_stats;
		this.spout = spout;
		this.task_id = task_id;
		this.report_error = _report_error;

		ackerNum = JStormUtils.parseInt(storm_conf
				.get(Config.TOPOLOGY_ACKER_EXECUTORS));
		isDebug = JStormUtils.parseBoolean(
				storm_conf.get(Config.TOPOLOGY_DEBUG), false);

		random = new Random();
		random.setSeed(System.currentTimeMillis());

		String componentId = topology_context.getThisComponentId();
		emitTotalTimer = Metrics.registerTimer(JStormServerUtils.getName(componentId, task_id), 
				MetricDef.EMIT_TIME, String.valueOf(task_id), Metrics.MetricType.TASK);
	}

	@Override
	public List<Integer> emit(String streamId, List<Object> tuple,
			Object messageId) {
		return sendSpoutMsg(streamId, tuple, messageId, null);
	}

	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple,
			Object messageId) {
		sendSpoutMsg(streamId, tuple, messageId, taskId);
	}

	private List<Integer> sendSpoutMsg(String out_stream_id,
			List<Object> values, Object message_id, Integer out_task_id) {

		emitTotalTimer.start();

		try {
			java.util.List<Integer> out_tasks = null;
			if (out_task_id != null) {
				out_tasks = sendTargets.get(out_task_id, out_stream_id, values);
			} else {
				out_tasks = sendTargets.get(out_stream_id, values);
			}

			if (out_tasks.size() == 0) {
				// don't need send tuple to other task
				return out_tasks;
			}
			List<Long> ackSeq = new ArrayList<Long>();
			Boolean needAck = (message_id != null) && (ackerNum > 0);

			//This change storm logic
			// Storm can't make sure root_id is unique
			// storm's logic is root_id = MessageId.generateId(random);
			// when duplicate root_id, it will miss call ack/fail
			Long root_id = MessageId.generateId(random);
			if (needAck) {
				while(pending.containsKey(root_id) == true) {
					root_id = MessageId.generateId(random);
				}
			} 
			for (Integer t : out_tasks) {
				MessageId msgid;
				if (needAck) {
					//Long as = MessageId.generateId();
					Long as = MessageId.generateId(random);
					msgid = MessageId.makeRootId(root_id, as);
					ackSeq.add(as);
				} else {
					msgid = MessageId.makeUnanchored();
				}

				TupleImplExt tp = new TupleImplExt(topology_context, values,
						task_id, out_stream_id, msgid);
				tp.setTargetTaskId(t);
				transfer_fn.transfer(tp);

			}

			if (needAck) {

				TupleInfo info = new TupleInfo();
				info.setStream(out_stream_id);
				info.setValues(values);
				info.setMessageId(message_id);
				info.setTimestamp(System.currentTimeMillis());

				pending.putHead(root_id, info);

				List<Object> ackerTuple = JStormUtils.mk_list((Object) root_id,
						JStormUtils.bit_xor_vals(ackSeq), task_id);

				UnanchoredSend.send(topology_context, sendTargets, transfer_fn,
						Acker.ACKER_INIT_STREAM_ID, ackerTuple);

			} else if (message_id != null) {
				TupleInfo info = new TupleInfo();
				info.setStream(out_stream_id);
				info.setValues(values);
				info.setMessageId(message_id);
				info.setTimestamp(0);

				AckSpoutMsg ack = new AckSpoutMsg(spout, info, task_stats,
						isDebug);
				ack.run();

			}

			return out_tasks;
		} finally {
			emitTotalTimer.stop();
		}

	}

	@Override
	public void reportError(Throwable error) {
		// TODO Auto-generated method stub
		report_error.report(error);
	}

}
