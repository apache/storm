package com.alibaba.jstorm.task.execute.spout;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.utils.ExpiredCallback;
import com.alibaba.jstorm.utils.JStormUtils;

public class SpoutTimeoutCallBack<K, V> implements
	ExpiredCallback<K, V> {
	private static Logger LOG = Logger.getLogger(SpoutTimeoutCallBack.class);

	private DisruptorQueue disruptorEventQueue;
	private backtype.storm.spout.ISpout spout;
	private Map storm_conf;
	private CommonStatsRolling task_stats;
	private boolean isDebug;

	public SpoutTimeoutCallBack(DisruptorQueue disruptorEventQueue,
			backtype.storm.spout.ISpout _spout, Map _storm_conf,
			CommonStatsRolling stat) {
		this.storm_conf = _storm_conf;
		this.disruptorEventQueue = disruptorEventQueue;
		this.spout = _spout;
		this.task_stats = stat;
		this.isDebug = JStormUtils.parseBoolean(
				storm_conf.get(Config.TOPOLOGY_DEBUG), false);
	}

	/**
	 * pending.put(root_id, JStormUtils.mk_list(message_id, TupleInfo, ms));
	 */
	@Override
	public void expire(K key, V val) {
		if (val == null) {
			return;
		}
		try {
			TupleInfo tupleInfo = (TupleInfo) val;
			FailSpoutMsg fail = new FailSpoutMsg(key, spout, (TupleInfo) tupleInfo,
					task_stats, isDebug);

			disruptorEventQueue.publish(fail);
		} catch (Exception e) {
			LOG.error("expire error", e);
		}
	}
}
