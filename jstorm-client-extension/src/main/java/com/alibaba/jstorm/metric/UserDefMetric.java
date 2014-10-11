package com.alibaba.jstorm.metric;

import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.io.Serializable;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.alibaba.jstorm.client.metric.MetricCallback;
import com.alibaba.jstorm.metric.MetricInfo;


/**
 * /storm-zk-root/Monitor/{topologyid}/UserDefMetrics/{workerid} data
 */
public class UserDefMetric {

	private static final long serialVersionUID = 4547327064057659279L;
	
	private Map<String, Gauge<?>> gaugeMap = new HashMap<String, Gauge<?>>();
	private Map<String, Counter> counterMap = new HashMap<String, Counter>();
	private Map<String, Histogram> histogramMap = new HashMap<String, Histogram>();
	private Map<String, Timer> timerMap = new HashMap<String, Timer>();
	private Map<String, Meter> meterMap = new HashMap<String, Meter>();
	private Map<String, MetricCallback> callbacks = new HashMap<String, MetricCallback>();
	
	public UserDefMetric() {
	}
	
	public Map<String, Gauge<?>> getGauge() {
		return this.gaugeMap;
	}
	public  void registerCallback(MetricCallback callback, String name) {
		if (callbacks.containsKey(name) != true) {
		    callbacks.put(name, callback);
		}
	}
	public  void unregisterCallback(String name) {
		callbacks.remove(name);
	}
	public  Map<String, MetricCallback> getCallbacks() {
		return callbacks;
	}
	public void addToGauge(String name, Gauge<?> gauge) {
		gaugeMap.put(name, gauge);
	}
	
	public Map<String, Counter> getCounter() {
		return this.counterMap;
	}
	
	public void addToCounter(String name, Counter counter) {
		counterMap.put(name, counter);
	}
	
	public Map<String, Histogram> getHistogram() {
		return this.histogramMap;
	}
	
	public void addToHistogram(String name, Histogram histogram) {
		histogramMap.put(name, histogram);
	}
	
	
	public Map<String, Timer> getTimer() {
		return this.timerMap;
	}
	
	public void addToTimer(String name, Timer timer) {
		timerMap.put(name, timer);
	}
	
	public Map<String, Meter> getMeter() {
		return this.meterMap;
	}
	
	public void addToMeter(String name, Meter meter) {
		meterMap.put(name, meter);
	}
	
	public void remove(String name) {
		if (gaugeMap.containsKey(name)) {
			gaugeMap.remove(name);
		} else if (counterMap.containsKey(name)) {
			counterMap.remove(name);
		} else if (histogramMap.containsKey(name)) {
			histogramMap.remove(name);
		} else if (timerMap.containsKey(name)) {
			timerMap.remove(name);
		} else if (meterMap.containsKey(name)) {
			meterMap.remove(name);
		}
		
		if (callbacks.containsKey(name)) {
			callbacks.remove(name);
		}
	}

}