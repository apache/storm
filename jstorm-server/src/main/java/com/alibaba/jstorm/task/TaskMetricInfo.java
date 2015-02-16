package com.alibaba.jstorm.task;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricInfo;
import com.alibaba.jstorm.metric.Metrics.QueueGauge;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.task.execute.spout.TimerRatio;

/**
 * /storm-zk-root/Monitor/{topologyid}/{taskid} data
 */
public class TaskMetricInfo implements Serializable {
	public static Logger LOG = Logger.getLogger(TaskMetricInfo.class);
	
	private static final long serialVersionUID = 7645367099257857979L;
	private String taskId;
	private String component;

    private Map<String, Double> gaugeData;
    private Map<String, Double> counterData;
    private Map<String, Double> meterData;
    private Map<String, Double> timerData;
    private Map<String, Double> histogramData;

	private static final double FULL_RATIO = 100.0;
	
	public static final String QEUEU_IS_FULL = "queue is full";

	public TaskMetricInfo(String taskId, String component) {
		this.taskId    = taskId;
		this.component = component;
		
		this.gaugeData     = new HashMap<String, Double>();
		this.counterData   = new HashMap<String, Double>();
		this.meterData     = new HashMap<String, Double>();
		this.timerData     = new HashMap<String, Double>();
		this.histogramData = new HashMap<String, Double>();
	}
	
	public String getTaskId() {
		return taskId;
	}
	
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	
	public String getComponent() {
		return this.component;
	}
	
	public void setComponent(String component) {
		this.component = component;
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
	
	public void updateMetricData(MetricInfo metricInfo) {
		String name = metricInfo.getName();
		Metric metric = metricInfo.getMetric();
		if (metric instanceof QueueGauge) {
			//covert to %
			float queueRatio = (((QueueGauge) metric).getValue())*100;
			double value = JStormUtils.formatDoubleDecPoint2((double)queueRatio);
            gaugeData.put(name, value);
		} else if ( (metric instanceof Gauge<?>) ||
				(metric instanceof TimerRatio)) {
			Double value = JStormUtils.convertToDouble(((Gauge) metric).getValue());
			if (value == null) {
				LOG.warn("gauge value is null or unknow type.");
			} else {
				value = JStormUtils.formatDoubleDecPoint4(value);
				gaugeData.put(name, value);
			}
	    } else if (metric instanceof Timer) {
			Snapshot snapshot = ((Timer) metric).getSnapshot();
			//Covert from ns to ms
			Double value = JStormUtils.formatDoubleDecPoint4(
					(snapshot.getMean())/1000000);
			timerData.put(name, value);
		} else if (metric instanceof Counter) {
		    Long value = ((Counter) metric).getCount();
		    counterData.put(name, value.doubleValue());
		} else if (metric instanceof Meter) {
			Double value = JStormUtils.formatDoubleDecPoint4(
					((Meter) metric).getMeanRate());
			meterData.put(name, value);
		} else if (metric instanceof Histogram) {
			Snapshot snapshot = ((Histogram) metric).getSnapshot();
			Double value = JStormUtils.formatDoubleDecPoint4(
					snapshot.getMean());
			histogramData.put(name, value);
		} else {
			LOG.warn("Unknown metric type, name:" + name);
		}
	}
	
	public List<String> anyQueueFull() {
		List<String> ret = new ArrayList<String>();
		String taskInfo = component + "-" + taskId + ": ";
	    if (gaugeData.get(MetricDef.DESERIALIZE_QUEUE) == FULL_RATIO) {
	    	ret.add(taskInfo + "deserialize-" + QEUEU_IS_FULL);
	    } else if (gaugeData.get(MetricDef.SERIALIZE_QUEUE) == FULL_RATIO)
	    {
	    	ret.add(taskInfo + "serialize-" + QEUEU_IS_FULL);
	    } else if (gaugeData.get(MetricDef.EXECUTE_QUEUE) == FULL_RATIO) {
	    	ret.add(taskInfo + "execute-" + QEUEU_IS_FULL);
	    }
	    return ret;
	}
	
	@Override
    public String toString() {
    	return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
