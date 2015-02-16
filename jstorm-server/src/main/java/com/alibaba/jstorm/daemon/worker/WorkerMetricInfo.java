package com.alibaba.jstorm.daemon.worker;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;

import com.alibaba.jstorm.metric.MetricInfo;
import com.alibaba.jstorm.metric.Metrics.QueueGauge;
import com.alibaba.jstorm.utils.JStormUtils;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;


/**
 * /storm-zk-root/Monitor/{topologyid}/{workerid} data
 */
public class WorkerMetricInfo implements Serializable {
	private static Logger LOG = Logger.getLogger(WorkerMetricInfo.class);
	
	private static final long serialVersionUID = 7745327094257659471L;
	
	private String hostName;
	private Integer port;
    
    private long  usedMem;
    private double usedCpu;
    
    private Map<String, Double> gaugeData;
    private Map<String, Double> counterData;
    private Map<String, Double> meterData;
    private Map<String, Double> timerData;
    private Map<String, Double> histogramData;
    
    private static final String METRIC_SEPERATOR = "-";
    
    public WorkerMetricInfo(String hostName, Integer port) {
    	this.hostName = hostName;
    	this.port = port;
    	
		this.gaugeData     = new HashMap<String, Double>();
		this.counterData   = new HashMap<String, Double>();
		this.meterData     = new HashMap<String, Double>();
		this.timerData     = new HashMap<String, Double>();
		this.histogramData = new HashMap<String, Double>();
    }
    
    public void setHostName(String hostName) {
    	this.hostName = hostName;
    }
    
    public String getHostName() {
    	return this.hostName;
    }
    
    public void setPort(Integer port) {
    	this.port = port;
    }
    
    public Integer getPort() {
    	return this.port;
    }
    
    public void setUsedMem(long usedMem) {
    	this.usedMem = usedMem;
    }
    
    public long getUsedMem() {
    	return this.usedMem;
    }
    
    public void setUsedCpu(double usedCpu) {
    	this.usedCpu = usedCpu;
    }
    
    public double getUsedCpu() {
    	return this.usedCpu;
    }
    
	public Map<String, Double> getGaugeData() {
		return gaugeData;
	}
	
	public Map<String, Double> getCounterData() {
		return counterData;
	}
	
	public Map<String, Double> getMeterData() {
		return meterData;
	}
	
	public Map<String, Double> getTimerData() {
		return timerData;
	}
	
	public Map<String, Double> getHistogramData() {
		return histogramData;
	}
    
	// There are some metrics that have same metric name, but just different prefix.
	// e.g for netty_send_time, full metric name is dest_ip:port-name
	// The metrics with same metric name will be sum here.
    public void updateMetricData(MetricInfo metricInfo) {
    	String name = metricInfo.getName();
		Metric metric = metricInfo.getMetric();
		LOG.debug("Metric name=" + name);
		if (metric instanceof QueueGauge) {
			//covert to %
			float queueRatio = (((QueueGauge) metric).getValue())*100;
			sum(gaugeData, name, (double)queueRatio);
		} else if (metric instanceof Gauge<?>) {
			Double value = JStormUtils.convertToDouble(((Gauge) metric).getValue());
			if (value == null) {
				LOG.warn("gauge value is null or unknow type.");
			} else {
				sum(gaugeData, name, value);
			}
        } else if (metric instanceof Timer) {
			Snapshot snapshot = ((Timer) metric).getSnapshot();
			//covert from ns to ms
			sum(timerData, name, (snapshot.getMean())/1000000);
		} else if (metric instanceof Counter) {
			Double value = ((Long) ((Counter) metric).getCount()).doubleValue();	
		    sum(counterData, name, value);
		} else if (metric instanceof Meter) {
			sum(meterData, name, ((Meter) metric).getMeanRate());
		} else if (metric instanceof Histogram) {
			Snapshot snapshot = ((Histogram)metric).getSnapshot();
			sum(histogramData, name, snapshot.getMean());
		} else {
			LOG.warn("Unknown metric type, name:" + name);
		}
    }
    
    private void sum(Map<String, Double> dataMap, String name, Double value) {
    	Double currentValue = dataMap.get(name);
		if (currentValue != null)
			value = value + currentValue;
		value = JStormUtils.formatDoubleDecPoint4(value);
		dataMap.put(name, value);
    }
    
    @Override
    public String toString() {
    	return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
    }
}