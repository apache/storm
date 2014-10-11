package com.alibaba.jstorm.task.execute.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.ISpout;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.comm.TupleInfo;

/**
 * Do the action after spout receive one failed tuple or sending tuple timeout
 * 
 * @author yannian/Longda
 * 
 */
public class FailSpoutMsg implements IAckMsg {
	private static Logger LOG = LoggerFactory.getLogger(FailSpoutMsg.class);
	private Object id;
	private ISpout spout;
	private TupleInfo tupleInfo;
	private CommonStatsRolling task_stats;
	private boolean isDebug = false;

	public FailSpoutMsg(Object id, ISpout _spout, TupleInfo _tupleInfo,
			CommonStatsRolling _task_stats, boolean _isDebug) {
		this.id = id;
		this.spout = _spout;
		this.tupleInfo = _tupleInfo;
		this.task_stats = _task_stats;
		this.isDebug = _isDebug;
	}

	public void run() {

		Object msg_id = tupleInfo.getMessageId();

		if (spout instanceof IFailValueSpout) {
			IFailValueSpout enhanceSpout = (IFailValueSpout) spout;
			enhanceSpout.fail(msg_id, tupleInfo.getValues());
		} else {
			spout.fail(msg_id);
		}

		task_stats.spout_failed_tuple(tupleInfo.getStream());

		if (isDebug) {
			LOG.info("Failed message rootId: {}, messageId:{} : {}", id, 
					msg_id, tupleInfo.getValues().toString());
		}
	}

}
