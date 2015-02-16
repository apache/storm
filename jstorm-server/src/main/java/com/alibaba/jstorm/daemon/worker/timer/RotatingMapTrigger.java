package com.alibaba.jstorm.daemon.worker.timer;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.utils.JStormUtils;

public class RotatingMapTrigger extends TimerTrigger {
	private static final Logger LOG = Logger
			.getLogger(RotatingMapTrigger.class);

	public RotatingMapTrigger(Map conf, String name, DisruptorQueue queue) {
		this.name = name;
		this.queue = queue;

		int msgTimeOut = JStormUtils.parseInt(
				conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
		frequence = (msgTimeOut) / (Acker.TIMEOUT_BUCKET_NUM - 1);
		if (frequence <= 0) {
			frequence = 1;
		}

		firstTime = JStormUtils.parseInt(
				conf.get(Config.SUPERVISOR_WORKER_START_TIMEOUT_SECS), 120);

		firstTime += frequence;
	}

	@Override
	public void updateObject() {
		this.object = new Tick(name);
	}

	public static final String ROTATINGMAP_STREAMID = "__rotating_tick";

	// In fact, RotatingMapTrigger can use TickTuple,
	// which set the stream ID is ROTATINGMAP_STREAMID
	// But in order to improve performance, JStorm use RotatingMapTrigger.Tick

	public static class Tick {
		private final long time;
		private final String name;

		public Tick(String name) {
			this.name = name;
			time = System.currentTimeMillis();
		}

		public long getTime() {
			return time;
		}

		public String getName() {
			return name;
		}

	}

}
