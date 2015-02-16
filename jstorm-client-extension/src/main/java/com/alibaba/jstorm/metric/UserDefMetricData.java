package com.alibaba.jstorm.metric;

import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.alibaba.jstorm.metric.metrdata.*;
import com.alibaba.jstorm.utils.JStormUtils;


/**
 * /storm-zk-root/Monitor/{topologyid}/user/{workerid} data
 */
public class UserDefMetricData implements Serializable {
	private static final Logger LOG = Logger.getLogger(UserDefMetricData.class);

	private static final long serialVersionUID = 954727168057659270L;
	
	private Map<String, GaugeData>     gaugeDataMap     = new HashMap<String, GaugeData>();
	private Map<String, CounterData>   counterDataMap   = new HashMap<String, CounterData>();
	private Map<String, TimerData>     timerDataMap     = new HashMap<String, TimerData>();
	private Map<String, MeterData>     meterDataMap     = new HashMap<String, MeterData>();
	private Map<String, HistogramData> histogramDataMap = new HashMap<String, HistogramData>();

	public UserDefMetricData() {
	}
	
	public Map<String, GaugeData> getGaugeDataMap() {
		return gaugeDataMap;
	}
	
	public Map<String, CounterData> getCounterDataMap() {
		return counterDataMap;
	}
	
	public Map<String, TimerData> getTimerDataMap() {
		return timerDataMap;
	}
	
	public Map<String, MeterData> getMeterDataMap() {
		return meterDataMap;
	}
	
	public Map<String, HistogramData> getHistogramDataMap() {
		return histogramDataMap;
	}
	
	public void updateFromGauge(Map<String, Gauge<?>> gaugeMap) {
		for(Entry<String, Gauge<?>> entry : gaugeMap.entrySet()) {
			try {
			    GaugeData gaugeData = new GaugeData();
			    gaugeData.setValue(JStormUtils.parseDouble(entry.getValue().getValue()));
			    gaugeDataMap.put(entry.getKey(), gaugeData);
			} catch (Throwable e) {
				LOG.error("updateFromGauge exception ", e);
			}
		}
	}
	
	public void updateFromCounter(Map<String, Counter> counterMap) {
		for(Entry<String, Counter> entry : counterMap.entrySet()) {
			CounterData counterData = new CounterData();
			counterData.setValue(entry.getValue().getCount());
			counterDataMap.put(entry.getKey(), counterData);
		}
	}
	
	public void updateFromMeterData(Map<String, Meter> meterMap) {
		for(Entry<String, Meter> entry : meterMap.entrySet()) {
			Meter meter = entry.getValue();
			MeterData meterData = new MeterData();
			meterData.setCount(meter.getCount());
			meterData.setMeanRate(meter.getMeanRate());
			meterData.setOneMinuteRate(meter.getOneMinuteRate());
			meterData.setFiveMinuteRate(meter.getFiveMinuteRate());
			meterData.setFifteenMinuteRate(meter.getFifteenMinuteRate());
			meterDataMap.put(entry.getKey(), meterData);
		}
	}
	
	public void updateFromHistogramData(Map<String, Histogram> histogramMap) {
		for(Entry<String, Histogram> entry : histogramMap.entrySet()) {
			Histogram histogram = entry.getValue();
			HistogramData histogramData = new HistogramData();
			histogramData.setCount(histogram.getCount());
			histogramData.setMax(histogram.getSnapshot().getMax());
			histogramData.setMin(histogram.getSnapshot().getMin());
			histogramData.setMean(histogram.getSnapshot().getMean());
			histogramData.setMedian(histogram.getSnapshot().getMedian());
			histogramData.setStdDev(histogram.getSnapshot().getStdDev());
			histogramData.setPercent75th(histogram.getSnapshot().get75thPercentile());
			histogramData.setPercent95th(histogram.getSnapshot().get95thPercentile());
			histogramData.setPercent98th(histogram.getSnapshot().get98thPercentile());
			histogramData.setPercent99th(histogram.getSnapshot().get99thPercentile());
			histogramData.setPercent999th(histogram.getSnapshot().get999thPercentile());
			histogramDataMap.put(entry.getKey(), histogramData);
		}
	}
	
	public void updateFromTimerData(Map<String, Timer> timerMap) {
		for(Entry<String, Timer> entry : timerMap.entrySet()) {
			Timer timer = entry.getValue();
			TimerData timerData = new TimerData();
			timerData.setCount(timer.getCount());
			timerData.setMax(timer.getSnapshot().getMax());
			timerData.setMin(timer.getSnapshot().getMin());
			timerData.setMean(timer.getSnapshot().getMean());
			timerData.setMedian(timer.getSnapshot().getMedian());
			timerData.setStdDev(timer.getSnapshot().getStdDev());
			timerData.setPercent75th(timer.getSnapshot().get75thPercentile());
			timerData.setPercent95th(timer.getSnapshot().get95thPercentile());
			timerData.setPercent98th(timer.getSnapshot().get98thPercentile());
			timerData.setPercent99th(timer.getSnapshot().get99thPercentile());
			timerData.setPercent999th(timer.getSnapshot().get999thPercentile());
			timerData.setMeanRate(timer.getMeanRate());
			timerData.setOneMinuteRate(timer.getOneMinuteRate());
			timerData.setFiveMinuteRate(timer.getFiveMinuteRate());
			timerData.setFifteenMinuteRate(timer.getFifteenMinuteRate());
			timerDataMap.put(entry.getKey(), timerData);
		}
	}
	
	@Override
    public String toString() {
    	return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
    }
}