package com.alibaba.jstorm.metric;

import com.codahale.metrics.Metric;

public class MetricInfo {
	private Metric metric;
	private String prefix;
	private String name;
		
	public MetricInfo(String prefix, String name, Metric metric) {
		this.prefix = prefix;
		this.name = name;
		this.metric = metric;
	}
	
	public String getPrefix() {
		return prefix;
	}
	
	public String getName() {
		return name;
	}
		
	public Metric getMetric() {
		return metric;
	}
}