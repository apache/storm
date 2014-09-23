package com.alibaba.jstorm.metric.metrdata;

import java.io.Serializable;


public class GaugeData implements Serializable {

	private static final long serialVersionUID = 954627168057659279L;
	
	private double value;
	
	public GaugeData () {
		value = 0.0;
	}
	
	public double getValue() {
		return value;
	}
	
	public void setValue(double value) {
		this.value = value;
	}
}