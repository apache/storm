package com.alibaba.jstorm.metric.metrdata;

import java.io.Serializable;


public class CounterData implements Serializable {

	private static final long serialVersionUID = 954627168057659219L;
	
	private long value;
	
	public CounterData () {
		value = 0l;
	}
	
	public long getValue() {
		return value;
	}
	
	public void setValue(long value) {
		this.value = value;
	}
}