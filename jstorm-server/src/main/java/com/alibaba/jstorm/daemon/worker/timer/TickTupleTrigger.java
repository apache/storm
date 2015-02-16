package com.alibaba.jstorm.daemon.worker.timer;

import org.apache.log4j.Logger;

import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.utils.TimeUtils;

public class TickTupleTrigger extends TimerTrigger {
	private static final Logger LOG = Logger.getLogger(TickTupleTrigger.class);

	TopologyContext topologyContext;

	public TickTupleTrigger(TopologyContext topologyContext, int frequence,
			String name, DisruptorQueue queue) {
		this.name = name;
		this.queue = queue;
		if (frequence <= 0) {
			LOG.warn(" The frequence of " + name  + " is invalid");
			frequence = 1;
		}
		this.firstTime = frequence;
		this.frequence = frequence;
		this.topologyContext = topologyContext;

	}

	@Override
	public void updateObject() {
		this.object = new TupleImplExt(topologyContext, new Values(
				TimeUtils.current_time_secs()), (int) Constants.SYSTEM_TASK_ID,
				Constants.SYSTEM_TICK_STREAM_ID);
	}

}
