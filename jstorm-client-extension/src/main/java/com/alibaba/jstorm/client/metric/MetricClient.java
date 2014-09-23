package com.alibaba.jstorm.client.metric;

import backtype.storm.task.TopologyContext;

import com.alibaba.jstorm.metric.Metrics;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.JStormHistogram;

public class MetricClient {

	private final int taskid;

	public MetricClient(TopologyContext context) {
		taskid = context.getThisTaskId();
	}
	
	private String getMetricName(Integer taskid, String name) {
		return "task-" + String.valueOf(taskid) + ":" + name;
	}

	public Gauge<?> registerGauge(String name, Gauge<?> gauge, MetricCallback<Gauge<?>> callback) {
		String userMetricName = getMetricName(taskid, name);
        Gauge<?> ret = Metrics.registerGauge(userMetricName, gauge);
        Metrics.registerUserDefine(userMetricName, gauge, callback);
		return ret;
	}

	public Counter registerCounter(String name, MetricCallback<Counter> callback) {
		String userMetricName = getMetricName(taskid, name);
        Counter ret = Metrics.registerCounter(userMetricName);
        Metrics.registerUserDefine(userMetricName, ret, callback);
		return ret;
	}
	
	public Meter registerMeter(String name, MetricCallback<Meter> callback) {
		String userMetricName = getMetricName(taskid, name);
		Meter ret = Metrics.registerMeter(userMetricName);
		Metrics.registerUserDefine(userMetricName, ret, callback);
		return ret;
	}
	
	public JStormTimer registerTimer(String name, MetricCallback<Timer> callback) {
		String userMetricName = getMetricName(taskid, name);
        JStormTimer ret = Metrics.registerTimer(userMetricName);
        Metrics.registerUserDefine(userMetricName, ret, callback);		
		return ret;
	}
	
	public JStormHistogram registerHistogram(String name, MetricCallback<Histogram> callback) {
		String userMetricName = getMetricName(taskid, name);
        JStormHistogram ret = Metrics.registerHistograms(userMetricName);
        Metrics.registerUserDefine(userMetricName, ret, callback);		
		return ret;
	}
	
	public boolean unregister(String name, Integer taskid) {
		String userMetricName = getMetricName(taskid, name);
		return Metrics.unregisterUserDefine(userMetricName);
	}

}
