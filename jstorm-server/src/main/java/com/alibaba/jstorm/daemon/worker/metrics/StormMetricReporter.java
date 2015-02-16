package com.alibaba.jstorm.daemon.worker.metrics;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.ScheduledReporter;
import com.esotericsoftware.minlog.Log;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.daemon.worker.WorkerMetricInfo;
import com.alibaba.jstorm.client.metric.MetricCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.container.SystemOperation;
import com.alibaba.jstorm.metric.MetricInfo;
import com.alibaba.jstorm.metric.Metrics;
import com.alibaba.jstorm.metric.UserDefMetric;
import com.alibaba.jstorm.metric.UserDefMetricData;
import com.alibaba.jstorm.task.TaskMetricInfo;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

public class StormMetricReporter extends ScheduledReporter {
	/**
     * Returns a new {@link Builder} for {@link StormMetricReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link StormMetricReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link CsvReporter} instances. Defaults to logging to {@code metrics}, not
     * using a marker, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Logger logger;
        private Marker marker;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private WorkerData workerData;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.logger = LoggerFactory.getLogger("metrics");
            this.marker = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.workerData = null;
        }

        /**
         * Log metrics to the given logger.
         *
         * @param logger an SLF4J {@link Logger}
         * @return {@code this}
         */
        public Builder outputTo(Logger logger) {
            this.logger = logger;
            return this;
        }

        /**
         * Mark all logged metrics with the given marker.
         *
         * @param marker an SLF4J {@link Marker}
         * @return {@code this}
         */
        public Builder markWith(Marker marker) {
            this.marker = marker;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }
        
        public Builder setWorkerData(WorkerData Data) {
        	this.workerData = Data;
        	return this;
        }
        /**
         * Builds a {@link StormMetricReporter} with the given properties.
         *
         * @return a {@link StormMetricReporter}
         */
        public StormMetricReporter build() {
            return new StormMetricReporter(registry, logger, marker, rateUnit,
                            durationUnit, filter, workerData);
        }
    }

    private final Logger logger;
    private final Marker marker;
    private WorkerData workerData;

    private StormMetricReporter(MetricRegistry registry,
                                Logger logger,
                                Marker marker,
                                TimeUnit rateUnit,
                                TimeUnit durationUnit,
                                MetricFilter filter,
                                WorkerData workerData) {
        super(registry, "logger-reporter", filter, rateUnit, durationUnit);
        this.logger = logger;
        this.marker = marker;
        this.workerData = workerData;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
    	boolean metricPerf = workerData.getMetricsReporter().isEnable();
    	
        processMetricData(gauges, counters, histograms, meters, timers, metricPerf);
        
        // update internal metrics data of jstorm task and worker, 
        // and user define metrics data to ZK
        updateMetricsDataToZK(metricPerf);
    }

    private <T extends Metric> void doCallback(Map<String, T> metrics) {
    	Map<String, MetricCallback> callbacks = Metrics.getUserDefMetric().getCallbacks();
    	String name = "";
    	try{
            for (Entry<String, T> entry : metrics.entrySet()) {
            	name = entry.getKey();
                MetricCallback callback = callbacks.get(entry.getValue());
                if (callback != null)
                    callback.callback(entry.getValue());
            }
	    } catch (Exception e) {
		    logger.error("Error when excuting the callbacks defined by user. CallBack Name=" + name, e);
	    }
    }

    private void processMetricData(SortedMap<String, Gauge> gauges,
                               SortedMap<String, Counter> counters,
                               SortedMap<String, Histogram> histograms,
                               SortedMap<String, Meter> meters,
                               SortedMap<String, Timer> timers,
                               boolean metricPerf) {  	
    	UserDefMetric userDefMetric = Metrics.getUserDefMetric();
    	
        for (Entry<String, Gauge> entry : gauges.entrySet()) {
                logGauge(entry.getKey(), entry.getValue());
        }

        for (Entry<String, Counter> entry : counters.entrySet()) {
                logCounter(entry.getKey(), entry.getValue());
        }

        for (Entry<String, Meter> entry : meters.entrySet()) {
                logMeter(entry.getKey(), entry.getValue());
        }

        if (metricPerf == true) {
            for (Entry<String, Histogram> entry : histograms.entrySet()) {
                logHistogram(entry.getKey(), entry.getValue());
            }
        
            for (Entry<String, Timer> entry : timers.entrySet()) {
        	    Map<String, Timer> timerMap = userDefMetric.getTimer();
                logTimer(entry.getKey(), entry.getValue());
            }
        }
 
    }

    private void logTimer(String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();
        
        logger.info(marker,
                    "type=TIMER, name={}, count={}, min={}, max={}, mean={}, stddev={}, median={}, " +
                            "p75={}, p95={}, p98={}, p99={}, p999={}, mean_rate={}, m1={}, m5={}, " +
                            "m15={}, rate_unit={}, duration_unit={}",
                    name,
                    timer.getCount(),
                    convertDuration(snapshot.getMin()),
                    convertDuration(snapshot.getMax()),
                    convertDuration(snapshot.getMean()),
                    convertDuration(snapshot.getStdDev()),
                    convertDuration(snapshot.getMedian()),
                    convertDuration(snapshot.get75thPercentile()),
                    convertDuration(snapshot.get95thPercentile()),
                    convertDuration(snapshot.get98thPercentile()),
                    convertDuration(snapshot.get99thPercentile()),
                    convertDuration(snapshot.get999thPercentile()),
                    convertRate(timer.getMeanRate()),
                    convertRate(timer.getOneMinuteRate()),
                    convertRate(timer.getFiveMinuteRate()),
                    convertRate(timer.getFifteenMinuteRate()),
                    getRateUnit(),
                    getDurationUnit());

    }

    private void logMeter(String name, Meter meter) {
        logger.info(marker,
                    "type=METER, name={}, count={}, mean_rate={}, m1={}, m5={}, m15={}, rate_unit={}",
                    name,
                    meter.getCount(),
                    convertRate(meter.getMeanRate()),
                    convertRate(meter.getOneMinuteRate()),
                    convertRate(meter.getFiveMinuteRate()),
                    convertRate(meter.getFifteenMinuteRate()),
                    getRateUnit());
    }

    private void logHistogram(String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();
        logger.info(marker,
                    "type=HISTOGRAM, name={}, count={}, min={}, max={}, mean={}, stddev={}, " +
                            "median={}, p75={}, p95={}, p98={}, p99={}, p999={}",
                    name,
                    histogram.getCount(),
                    snapshot.getMin(),
                    snapshot.getMax(),
                    snapshot.getMean(),
                    snapshot.getStdDev(),
                    snapshot.getMedian(),
                    snapshot.get75thPercentile(),
                    snapshot.get95thPercentile(),
                    snapshot.get98thPercentile(),
                    snapshot.get99thPercentile(),
                    snapshot.get999thPercentile());
    }

    private void logCounter(String name, Counter counter) {
        logger.info(marker, "type=COUNTER, name={}, count={}", name, counter.getCount());
    }

    private void logGauge(String name, Gauge gauge) {
        logger.info(marker, "type=GAUGE, name={}, value={}", name, gauge.getValue());
    }

    @Override
    protected String getRateUnit() {
        return "events/" + super.getRateUnit();
    }
    
    private void updateMetricsDataToZK(boolean metricPerf) {
    	Map<String, List<MetricInfo>> taskMetricMap = Metrics.getTaskMetricMap();
    	List<MetricInfo> workerMetricList = Metrics.getWorkerMetricList();
    	
    	updateTaskMetricsToZK(taskMetricMap, metricPerf);
    	updateWorkerMetricsToZK(workerMetricList, metricPerf);
    	updateUserDefMetricsToZK(metricPerf);
    }
    
    private void updateTaskMetricsToZK(Map<String, List<MetricInfo>> metricMap, boolean metricPerf) {
        StormClusterState clusterState = workerData.getZkCluster();
    	String topologyId = workerData.getTopologyId();
    	
    	for(Entry<String, List<MetricInfo>> entry : metricMap.entrySet()) {
    		String taskId = entry.getKey();
    		List<MetricInfo> MetricList = entry.getValue();
    		
    		try {
    			String component = workerData.getTasksToComponent().get(Integer.valueOf(taskId));
        		TaskMetricInfo taskMetricInfo = new TaskMetricInfo(taskId, component);
        		
    	  	    for(MetricInfo metricInfo : MetricList) {
    	  	        if(metricPerf == false && ((metricInfo.getMetric() instanceof Timer) || 
    	  	                (metricInfo.getMetric() instanceof Histogram)))
    	  	            continue;
    			    taskMetricInfo.updateMetricData(metricInfo);
    	  	    }
    	  	    
    	  	    List<String> errors = taskMetricInfo.anyQueueFull();
    	  	    if (errors.size() > 0) {
    	  	    	for (String error : errors)
    	  	            clusterState.report_task_error(topologyId, Integer.valueOf(taskId), error);
    	  	    }
    	  	    
    		    clusterState.update_task_metric(topologyId, taskId, taskMetricInfo);
    		} catch(Exception e) {
    			logger.error(marker, "Failed to update metrics data in ZK for topo-{} task-{}.",
			              topologyId, taskId, e);
			}
    	}
    }
    
    public Double getCpuUsage() {
		Double value = 0.0;
		String output = null;
		try {
	        String pid = JStormUtils.process_pid();
	        output = SystemOperation.exec("top -b -n 1 | grep " + pid);
	        String subStr = output.substring(output.indexOf("S") + 1);
	        for(int i = 0; i < subStr.length(); i++) {
	            char ch = subStr.charAt(i);
	            if (ch != ' ') {
	            	subStr = subStr.substring(i);
	            	break;
	            	}
	        }
	        String usedCpu = subStr.substring(0, subStr.indexOf(" "));
	        value = Double.valueOf(usedCpu);
	    } catch (Exception e) {
	    	logger.warn("Failed to get cpu usage ratio.");
	    	if (output != null)
	    		logger.warn("Output string is \"" + output + "\"");
	    	value = 0.0;
	    } 
		
		return value;
	}
    
    private void updateWorkerMetricsToZK(List<MetricInfo> metricList, boolean metricPerf) {
        StormClusterState clusterState = workerData.getZkCluster();
    	String topologyId = workerData.getTopologyId();
    	String hostName;

    	hostName = NetWorkUtils.ip();
    	String workerId = hostName + ":" + workerData.getPort();
    	
    	WorkerMetricInfo workerMetricInfo = new WorkerMetricInfo(hostName, workerData.getPort());
    	try {
    		//Set metrics data
    	    for(MetricInfo metricInfo : metricList) {
    	        if(metricPerf == false && ((metricInfo.getMetric() instanceof Timer) || 
                        (metricInfo.getMetric() instanceof Histogram)))
                    continue;
    			workerMetricInfo.updateMetricData(metricInfo);	
    		}
    	    
    	    //Set cpu & memory usage
    	    Runtime rt=Runtime.getRuntime();
    	    long usedMem = rt.totalMemory() - rt.freeMemory();
    	    workerMetricInfo.setUsedMem(usedMem);   
    	    
    	    workerMetricInfo.setUsedCpu(getCpuUsage());
    	    		
    	    clusterState.update_worker_metric(topologyId, workerId, workerMetricInfo);
    	} catch(Exception e) {
    		logger.error(marker, "Failed to update metrics data in ZK for topo-{} idStr-{}.",
			          topologyId, workerId, e);
    	}
    }
    
    private void updateUserDefMetricsToZK(boolean metricPerf) {
    	StormClusterState clusterState = workerData.getZkCluster();
    	String topologyId = workerData.getTopologyId();
    	String hostName =JStormServerUtils.getHostName(workerData.getConf());
    	String workerId = hostName + ":" + workerData.getPort();
    	
    	UserDefMetric userDefMetric = Metrics.getUserDefMetric();
    	UserDefMetricData userDefMetricData = new UserDefMetricData();
    	userDefMetricData.updateFromGauge(userDefMetric.getGauge());
    	userDefMetricData.updateFromCounter(userDefMetric.getCounter());
    	userDefMetricData.updateFromMeterData(userDefMetric.getMeter());
    	// If metrics performance is disable, Timer & Histogram metrics will not be monitored,
    	// and the corresponding metrics data will not be sent to ZK either.
    	if (metricPerf) {
    	    userDefMetricData.updateFromHistogramData(userDefMetric.getHistogram());
    	    userDefMetricData.updateFromTimerData(userDefMetric.getTimer());
    	}

    	try {
    	    clusterState.update_userDef_metric(topologyId, workerId, userDefMetricData);
    	} catch(Exception e) {
    		logger.error(marker, "Failed to update user define metrics data in ZK for topo-{} idStr-{}.",
			          topologyId, workerId, e);
    	}
    	
    	//Do callbacks defined by user
    	doCallback(userDefMetric.getGauge());
    	doCallback(userDefMetric.getCounter());
    	doCallback(userDefMetric.getMeter());
    	if (metricPerf) {
    	    doCallback(userDefMetric.getHistogram());
    	    doCallback(userDefMetric.getTimer());
    	}
    }
    
}