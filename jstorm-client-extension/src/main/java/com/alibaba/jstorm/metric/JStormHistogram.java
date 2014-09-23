package com.alibaba.jstorm.metric;

import com.codahale.metrics.Histogram;

public class JStormHistogram {
	private static boolean isEnable = true;

	public static boolean isEnable() {
		return isEnable;
	}

	public static void setEnable(boolean isEnable) {
		JStormHistogram.isEnable = isEnable;
	}

	private Histogram instance;
	private String    name;

	public JStormHistogram(String name, Histogram instance) {
		this.name = name;
		this.instance = instance;
	}

	public void update(int value) {
		if (isEnable == true) {
			instance.update(value);
		}
	}

	public void update(long value) {
		if (isEnable == true) {
			instance.update(value);
		}
	}
	
	public Histogram getInstance() {
		return instance;
	}
}
