package com.alibaba.jstorm.daemon.worker.timer;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;

import backtype.storm.utils.DisruptorQueue;

import com.lmax.disruptor.InsufficientCapacityException;

public class TimerTrigger implements Runnable {
	private static final Logger LOG = Logger.getLogger(TimerTrigger.class);

	private static ScheduledExecutorService threadPool;

	public static void setScheduledExecutorService(
			ScheduledExecutorService scheduledExecutorService) {
		threadPool = scheduledExecutorService;
	}

	protected String name;
	protected int firstTime;
	protected int frequence;
	protected DisruptorQueue queue;
	protected Object object;
	protected boolean block = true;

	public void register() {
		threadPool.scheduleAtFixedRate(this, firstTime, frequence,
				TimeUnit.SECONDS);
		LOG.info("Successfully register timer " + this);
	}

	public void updateObject() {

	}

	@Override
	public void run() {

		try {
			updateObject();

			if (object == null) {
				LOG.info("Timer " + name + " 's object is null ");
				return;
			}
			queue.publish(object, block);
		}catch(InsufficientCapacityException e) {
			LOG.warn("Failed to public timer event to " + name);
			return;
		}catch (Exception e) {
			LOG.warn("Failed to public timer event to " + name, e);
			return;
		}

		LOG.debug(" Trigger timer event to " + name);

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getFirstTime() {
		return firstTime;
	}

	public void setFirstTime(int firstTime) {
		this.firstTime = firstTime;
	}

	public int getFrequence() {
		return frequence;
	}

	public void setFrequence(int frequence) {
		this.frequence = frequence;
	}

	public DisruptorQueue getQueue() {
		return queue;
	}

	public void setQueue(DisruptorQueue queue) {
		this.queue = queue;
	}

	public Object getObject() {
		return object;
	}

	public void setObject(Object object) {
		this.object = object;
	}

	public boolean isBlock() {
		return block;
	}

	public void setBlock(boolean block) {
		this.block = block;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

}
