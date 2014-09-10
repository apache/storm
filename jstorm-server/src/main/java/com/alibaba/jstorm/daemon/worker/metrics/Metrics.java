package com.alibaba.jstorm.daemon.worker.metrics;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.daemon.worker.Worker;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

public class Metrics {

	private static final Logger LOG = Logger.getLogger(Metrics.class);
	private static final Logger DEFAULT_LOG = Logger.getLogger(Worker.class);

	private static final MetricRegistry metrics = new MetricRegistry();

	private static final MetricRegistry jstack = new MetricRegistry();
	
	static  {
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
	
	public static void unregister(String name) {
		metrics.remove(name);
	}
	
	public static <T extends Metric> T register(String name, T metric)
			throws IllegalArgumentException {
		LOG.info("Register Metric " + name);
		return metrics.register(name, metric);
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
				DEFAULT_LOG.info("Queue " + name + "is full " + ret);
			}
			
			return ret;
		}
		
	}

	public static Gauge<Float> registerQueue(String name, DisruptorQueue queue) {
		LOG.info("Register Metric " + name);
		return metrics.register(name, new QueueGauge(name, queue));
	}

	public static Counter registerCounter(String name) {
		LOG.info("Register Metric " + name);
		return metrics.counter(name);
	}

	public static Meter registerMeter(String name) {
		LOG.info("Register Metric " + name);
		Meter ret = metrics.meter(name);

		return ret;
	}

	public static JStormHistogram registerHistograms(String name) {
		LOG.info("Register Metric " + name);
		Histogram instance = metrics.histogram(name);
		
		return new JStormHistogram(name, instance);
	}

	public static JStormTimer registerTimer(String name) {
		LOG.info("Register Metric " + name);
		
		Timer instance = metrics.timer(name);
		return new JStormTimer(name, instance);
	}

}
