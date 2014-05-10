package com.alibaba.jstorm.utils;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * statistics tuples: sampling event
 * 
 * 
 * @author yannian/Longda
 * 
 */
public class EventSampler {
	private volatile int freq;
	private AtomicInteger i = new AtomicInteger(0);
	private volatile int target;
	private Random r = new Random();

	public EventSampler(int freq) {
		this.freq = freq;
		this.target = r.nextInt(freq);

		if (freq / 4 > 1) {
			intervalCheck.setInterval(freq / 4);
		}
	}

	/**
	 * select 1/freq
	 * 
	 * @return
	 */
	public boolean countCheck() {
		i.incrementAndGet();
		if (i.get() > freq) {
			target = r.nextInt(freq);
			i.set(0);
		}
		if (i.get() == target) {
			return true;
		}
		return false;
	}

	private AtomicInteger counter = new AtomicInteger(0);
	private AtomicLong sum = new AtomicLong(0);
	private IntervalCheck intervalCheck = new IntervalCheck();

	public Integer tpsCheck() {
		int send = counter.incrementAndGet();

		Double pastSeconds = intervalCheck.checkAndGet();
		if (pastSeconds != null) {
			counter.set(0);

			return Integer.valueOf((int) (send / pastSeconds));

		}

		return null;
	}

	public Integer timesCheck() {
		int send = counter.incrementAndGet();

		Double pastSeconds = intervalCheck.checkAndGet();
		if (pastSeconds != null) {
			counter.set(0);

			return send;

		}

		return null;
	}
	
	public Pair<Integer, Double> avgCheck(long one) {
		int send = counter.incrementAndGet();
		long total = sum.addAndGet(one);

		Double pastSeconds = intervalCheck.checkAndGet();
		if (pastSeconds != null) {
			counter.set(0);
			sum.set(0);
			
			Double avg = Double.valueOf(0);
			if (send != 0) {
				avg = ((double)total)/send;
			}
			 
			
			return new Pair<Integer, Double>(send, avg);

		}

		return null;
	}
}
