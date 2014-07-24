package com.alipay.dw.jstorm.example;

import java.io.Serializable;

public class IntervalCheck implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8952971673547362883L;

	long lastCheck = System.currentTimeMillis();

	// default interval is 1 second
	long interval = 1;

	/*
	 * if last check time is before interval seconds, return true, otherwise
	 * return false
	 */
	public boolean check() {
		return checkAndGet() != null;
	}

	/**
	 * 
	 * @return
	 */
	public Double checkAndGet() {
		long now = System.currentTimeMillis();

		synchronized (this) {
			if (now >= interval * 1000 + lastCheck) {
				double pastSecond = ((double) (now - lastCheck)) / 1000;
				lastCheck = now;
				return pastSecond;
			}
		}

		return null;
	}

	public long getInterval() {
		return interval;
	}

	public void setInterval(long interval) {
		this.interval = interval;
	}

	public void adjust(long addTimeMillis) {
		lastCheck += addTimeMillis;
	}
	
	public void start() {
		lastCheck = System.currentTimeMillis();
	}
}
