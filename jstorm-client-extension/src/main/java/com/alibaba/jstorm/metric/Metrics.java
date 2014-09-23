package com.alibaba.jstorm.metric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.client.metric.MetricCallback;
//import com.alibaba.jstorm.daemon.worker.Worker;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

public class Metrics {

	public enum MetricType {
		TASK, WORKER
	}

	private static final Logger LOG = Logger.getLogger(Metrics.class);
	//private static final Logger DEFAULT_LOG = Logger.getLogger(Worker.class);

	private static final MetricRegistry metrics = new MetricRegistry();

	private static final MetricRegistry jstack = new MetricRegistry();

	private static Map<String, List<MetricInfo>> taskMetricMap = new ConcurrentHashMap<String, List<MetricInfo>>();
	private static List<MetricInfo> workerMetricList = new ArrayList<MetricInfo>();
	private static UserDefMetric userDefMetric = new UserDefMetric();

	static {
		try {
			registerAll("jvm-thread-state", new ThreadStatesGaugeSet());
			registerAll("jvm-mem", new MemoryUsageGaugeSet());
			registerAll("jvm-gc", new GarbageCollectorMetricSet());

			jstack.register("jstack", new MetricJstack());
		} catch (Exception e) {
			LOG.warn("Failed to regist jvm metrics");
		}
	}

	public static MetricRegistry getMetrics() {
		return metrics;
	}

	public static MetricRegistry getJstack() {
		return jstack;
	}
	
	public static UserDefMetric getUserDefMetric() {
		return userDefMetric;
	}
	
	public static boolean unregister(String name) {
		LOG.info("Unregister metric " + name);
		return metrics.remove(name);
	}
	
	public static boolean unregister(String prefix, String name, String id, Metrics.MetricType type) {
		String MetricName;
		if (prefix == null)
			MetricName = name;
		else
			MetricName = prefix + "-" + name;
		boolean ret = unregister(MetricName);
		
		if (ret == true) {
			List<MetricInfo> metricList = null;
		    if (type == MetricType.WORKER) {
		    	metricList = workerMetricList;
		    } else {
		    	metricList = taskMetricMap.get(id);
		    }
		    
		    boolean found = false;
		    if (metricList != null) {
		        for (MetricInfo metric : metricList) {
		            if(metric.getName().equals(name)) {
		            	if (prefix != null) { 
		            		if (metric.getPrefix().equals(prefix)) {
		            	        metricList.remove(metric);
		            	        found = true;
		                        break;
		            		}
		            	} else {
		            		if (metric.getPrefix() == null) {
		            		    metricList.remove(metric);
	            	            found = true;
	                            break;
		            		}
		            	}
		            }
	            }
		    }
		    if (found != true) 
		    	LOG.warn("Name " + name + " is not found when unregister from metricList");
		}
		return ret;
	}

	public static boolean unregisterUserDefine(String name) {
		boolean ret = unregister(name);
		
		if (ret == true) {
			userDefMetric.remove(name);
			userDefMetric.unregisterCallback(name);
		}
		
		return ret;
	}

	public static <T extends Metric> T register(String name, T metric)
			throws IllegalArgumentException {
		LOG.info("Register Metric " + name);
		return metrics.register(name, metric);
	}

	public static <T extends Metric> T register(String prefix, String name, T metric, 
			String idStr, MetricType metricType) throws IllegalArgumentException {
		String metricName;
		if (prefix == null) 
			metricName = name;
		else
			metricName = prefix + "-" + name;
		T ret = register(metricName, metric);
		updateMetric(prefix, name, metricType, ret, idStr);
		return ret;
	}

	public static void registerUserDefine(String name, Object metric, MetricCallback callback) {		
		if(metric instanceof Gauge<?>) {
            userDefMetric.addToGauge(name, (Gauge<?>)metric);
		} else if (metric instanceof Timer) {
            userDefMetric.addToTimer(name, (Timer)metric);
		} else if (metric instanceof Counter) {
			userDefMetric.addToCounter(name, (Counter)metric);
		} else if (metric instanceof Meter) {
			userDefMetric.addToMeter(name, (Meter)metric);
		} else if (metric instanceof Histogram) {
			userDefMetric.addToHistogram(name, (Histogram)metric);
		} else if (metric instanceof JStormTimer) {
			userDefMetric.addToTimer(name, ((JStormTimer)metric).getInstance());
	    } else if (metric instanceof JStormHistogram) {
	    	userDefMetric.addToHistogram(name, ((JStormHistogram)metric).getInstance());
	    } else {
			LOG.warn("registerUserDefine, unknow Metric type, name=" + name);
		}
		
		if (callback != null) {
			userDefMetric.registerCallback(callback, name);
		}
	}


	// copy from MetricRegistry
	public static void registerAll(String prefix, MetricSet metrics)
			throws IllegalArgumentException {
		for (Map.Entry<String, Metric> entry : metrics.getMetrics().entrySet()) {
			if (entry.getValue() instanceof MetricSet) {
				registerAll(MetricRegistry.name(prefix, entry.getKey()),
						(MetricSet) entry.getValue());
			} else {
				register(MetricRegistry.name(prefix, entry.getKey()),
						entry.getValue());
			}
		}
	}

	private static void updateMetric(String prefix, String name, MetricType metricType,
			Metric metric, String idStr) {
		Map<String, List<MetricInfo>> metricMap;
		List<MetricInfo> metricList;
		if (metricType == MetricType.TASK) {
			metricMap = taskMetricMap;
			metricList = metricMap.get(idStr);
			if (null == metricList) {
				metricList = new ArrayList<MetricInfo>();
				metricMap.put(idStr, metricList);
			}
		} else if (metricType == MetricType.WORKER) {
			metricList = workerMetricList;
		} else {
			LOG.error("updateMetricMap: unknown metric type");
			return;
		}

		MetricInfo metricInfo = new MetricInfo(prefix, name, metric);
		metricList.add(metricInfo);

	}

	public static Map<String, List<MetricInfo>> getTaskMetricMap() {
		return taskMetricMap;
	}

	public static List<MetricInfo> getWorkerMetricList() {
		return workerMetricList;
	}

	public static class QueueGauge implements Gauge<Float> {
		DisruptorQueue queue;
		String         name;
		
		public QueueGauge(String name, DisruptorQueue queue) {
			this.queue = queue;
			this.name = name;
		}
		
		@Override
		public Float getValue() {
			Float ret = queue.pctFull();
			if (ret > 0.8) {
				//DEFAULT_LOG.info("Queue " + name + "is full " + ret);
			}
			
			return ret;
		}
		
	}

	public static Gauge<Float> registerQueue(String name, DisruptorQueue queue) {
		LOG.info("Register Metric " + name);
		return metrics.register(name, new QueueGauge(name, queue));
	}
	
	public static Gauge<Float> registerQueue(String prefix, String name, DisruptorQueue queue,
			String idStr, MetricType metricType) {
		String metricName;
		if (prefix == null) 
			metricName = name;
		else
			metricName = prefix + "-" + name;
		Gauge<Float> ret = registerQueue(metricName, queue);
		updateMetric(prefix, name, metricType, ret, idStr);
		return ret;
	}

	public static Gauge<?> registerGauge(String name, Gauge<?> gauge) {
		LOG.info("Register Metric " + name);
		return metrics.register(name, gauge);
	}
	
	public static Counter registerCounter(String name) {
		LOG.info("Register Metric " + name);
		return metrics.counter(name);
	}
	
	public static Counter registerCounter(String prefix, String name,
			String idStr, MetricType metricType) {
		String metricName;
		if (prefix == null) 
			metricName = name;
		else
			metricName = prefix + "-" + name;
		Counter ret = registerCounter(metricName);
		updateMetric(prefix, name, metricType, ret, idStr);
		return ret;
	}

	public static Meter registerMeter(String name) {
		LOG.info("Register Metric " + name);
		return metrics.meter(name);
	}
	
	public static Meter registerMeter(String prefix, String name,
			String idStr, MetricType metricType) {
		String metricName;
		if (prefix == null) 
			metricName = name;
		else
			metricName = prefix + "-" + name;
		Meter ret = registerMeter(metricName);
		updateMetric(prefix, name, metricType, ret, idStr);
		return ret;
	}

	public static JStormHistogram registerHistograms(String name) {
		LOG.info("Register Metric " + name);
		Histogram instance = metrics.histogram(name);

		return new JStormHistogram(name, instance);
	}
	
	public static JStormHistogram registerHistograms(String prefix, String name,
			String idStr, MetricType metricType) {
		String metricName;
		if (prefix == null) 
			metricName = name;
		else
			metricName = prefix + "-" + name;
		JStormHistogram ret = registerHistograms(metricName);
		updateMetric(prefix, name, metricType, ret.getInstance(), idStr);
		return ret;
	}

	public static JStormTimer registerTimer(String name) {
		LOG.info("Register Metric " + name);
		
		Timer instance = metrics.timer(name);
		return new JStormTimer(name, instance);
	}
	
	public static JStormTimer registerTimer(String prefix, String name,
			String idStr, MetricType metricType) {
		String metricName;
		if (prefix == null) 
			metricName = name;
		else
			metricName = prefix + "-" + name;
		JStormTimer ret = registerTimer(metricName);
		updateMetric(prefix, name, metricType, ret.getInstance(), idStr);
		return ret;
	}

}
