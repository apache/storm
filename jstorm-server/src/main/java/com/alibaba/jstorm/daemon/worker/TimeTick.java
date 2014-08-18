package com.alibaba.jstorm.daemon.worker;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * Timely check whether topology is active or not from ZK
 * 
 * @author yannian/Longda
 * 
 */
public class TimeTick extends RunnableCallback {
	private static Logger LOG = Logger.getLogger(TimeTick.class);


	private AtomicBoolean active;

	private Integer frequence;
	private Integer firstSleep;

	private static Map<String, DisruptorQueue> queues = new HashMap<String, DisruptorQueue>();
	
	public static void registerTimer(String name, DisruptorQueue queue) {
		queues.put(name, queue);
	}
	
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

	@SuppressWarnings("rawtypes")
	public TimeTick(WorkerData workerData) {
		active = workerData.getActive();
		
		Map conf = workerData.getStormConf();
		int msgTimeOut = JStormUtils.parseInt(
				conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
		frequence = (msgTimeOut) / (Acker.TIMEOUT_BUCKET_NUM - 1);
		if (frequence <= 0) {
			frequence = 1;
		}
		
		firstSleep = JStormUtils.parseInt(
				conf.get(Config.SUPERVISOR_WORKER_START_TIMEOUT_SECS), 120);
		
		firstSleep += frequence;
		LOG.info("TimeTick frequence " + frequence);
	}
	
	private boolean isFirstTime = true;

	@Override
	public void run() {

		if (active.get() == false) {
			return;
		}

		if (isFirstTime == true) {
			isFirstTime = false;
			JStormUtils.sleepMs(firstSleep * 1000);
			LOG.info("Start TimeTick");
		}
		
		for (Entry<String, DisruptorQueue> entry: queues.entrySet()) {
			String name = entry.getKey();
			DisruptorQueue queue = entry.getValue();
			Tick tick = new Tick(name);
			queue.publish(tick);
			LOG.debug("Issue time tick " + name);
		}
		
	}

	@Override
	public Object getResult() {
		if (active.get()) {
			return frequence;
		}
		return -1;
	}
}
