package com.alibaba.jstorm.task.execute.spout;

import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Gauge;

/**
 * 
 * @author zhongyan.feng
 * @version $Id:
 */
public class TimerRatio implements Gauge<Double> {

	private long lastUpdateTime = 0;
	private long sum = 0;
	private long lastGaugeTime;

	public void init() {
		lastGaugeTime = System.currentTimeMillis();
	}

	public synchronized void start() {
		if (lastUpdateTime == 0) {
			lastUpdateTime = System.currentTimeMillis();
		}
	}

	public synchronized void stop() {
		if (lastUpdateTime != 0) {
			long now = System.currentTimeMillis();
			long cost = now - lastUpdateTime;
			lastUpdateTime = 0;
			sum += cost;
		}

	}

	@Override
	public Double getValue() {
		synchronized (this) {
			stop();

			long now = System.currentTimeMillis();
			long cost = now - lastGaugeTime;
			if (cost == 0) {
				return 1.0;
			}
			
			lastGaugeTime = now;
			double ratio = ((double) sum) / cost;
			sum = 0;
			return ratio;

		}

	}

}
