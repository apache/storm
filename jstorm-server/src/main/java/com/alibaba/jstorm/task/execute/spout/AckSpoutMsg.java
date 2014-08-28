package com.alibaba.jstorm.task.execute.spout;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.ISpout;

import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.comm.TupleInfo;

/**
 * The action after spout receive one ack tuple
 * 
 * @author yannian/Longda
 * 
 */
public class AckSpoutMsg implements IAckMsg {
	private static Logger LOG = LoggerFactory.getLogger(AckSpoutMsg.class);

	private ISpout spout;
	private Object msgId;
	private String stream;
	private long timeStamp;
	private List<Object> values;
	private CommonStatsRolling task_stats;
	private boolean isDebug = false;

	public AckSpoutMsg(ISpout _spout, TupleInfo tupleInfo,
			CommonStatsRolling _task_stats, boolean _isDebug) {

		this.task_stats = _task_stats;

		this.spout = _spout;
		this.isDebug = _isDebug;

		this.msgId = tupleInfo.getMessageId();
		this.stream = tupleInfo.getStream();
		this.timeStamp = tupleInfo.getTimestamp();
		this.values = tupleInfo.getValues();
	}

	public void run() {
		if (isDebug) {
			LOG.info("Acking message {}", msgId);
		}

		if (spout instanceof IAckValueSpout) {
			IAckValueSpout ackValueSpout = (IAckValueSpout) spout;
			ackValueSpout.ack(msgId, values);
		} else {
			spout.ack(msgId);
		}

		task_stats.spout_acked_tuple(stream, timeStamp);
	}

}
