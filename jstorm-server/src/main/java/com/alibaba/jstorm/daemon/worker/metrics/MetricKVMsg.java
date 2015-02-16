package com.alibaba.jstorm.daemon.worker.metrics;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.metric.UserDefMetricData;
import com.alibaba.jstorm.metric.metrdata.*;
import backtype.storm.generated.UserDefMetric;

//count metric data,and transform metric_data to Alimonitor message
public class MetricKVMsg {
	private static final Logger LOG = Logger.getLogger(MetricKVMsg.class);
	
	public enum MetricType{  
		count, min, max, mean, median, stddev, p75, p95, p98, p99, p999, mean_rate, m1, m5, m15
	}
	
	private Map<String, Double> gaugeMapKV = new HashMap<String, Double>(); //count value of Gauge
	private Map<String, Long> counterMapKV = new HashMap<String, Long>(); //count value of Counter
	private Map<String, Map<MetricType, Double>> histogramMapKV = new HashMap<String, Map<MetricType, Double>>(); // count data of Histogram
	private Map<String, Map<MetricType, Double>> timerMapKV = new HashMap<String, Map<MetricType, Double>>(); // count data of Timer
	private Map<String, Map<MetricType, Double>> meterMapKV = new HashMap<String, Map<MetricType, Double>>(); // count data of Meter
	private Map<String, Integer> countMap = new HashMap<String, Integer>();
	
	public Map<String, Object> convertToKVMap() {
		Map<String, Object> ret = new HashMap<String, Object>();
		
		addGaugeToKVMap(ret);
		addCounterToKVMap(ret);
		addHistogramToKVMap(ret);
		addTimerToKVMap(ret);
		addMeterToKVMap(ret);
		
		return ret;
	}
	
	public void addGaugeToKVMap(Map<String, Object> kVMap) {
        for (Entry<String, Double> entry : gaugeMapKV.entrySet()) {
        	kVMap.put(entry.getKey(), entry.getValue());
        }
	}
	
	public void addCounterToKVMap(Map<String, Object> kVMap) {
        for (Entry<String, Long> entry : counterMapKV.entrySet()) {
        	kVMap.put(entry.getKey(), entry.getValue());
        }
	}
	
	public void addHistogramToKVMap(Map<String, Object> kVMap) {
        for (Entry<String, Map<MetricType, Double>> entry : histogramMapKV.entrySet()) {
        	String name = entry.getKey();
        	Map<MetricType, Double> typeMap = entry.getValue(); 

        	for (Entry<MetricType, Double> typeEntry : typeMap.entrySet()) {        	
        	    kVMap.put(name+ "_" + typeEntry.getKey().toString(), typeEntry.getValue()); 
        	}
        }
	}
	
	public void addTimerToKVMap(Map<String, Object> kVMap) {
        for (Entry<String, Map<MetricType, Double>> entry : timerMapKV.entrySet()) {
        	String name = entry.getKey();
        	Map<MetricType, Double> typeMap = entry.getValue(); 

        	for (Entry<MetricType, Double> typeEntry : typeMap.entrySet()) {        	
        	    kVMap.put(name+ "_" + typeEntry.getKey().toString(), typeEntry.getValue()); 
        	}
        }
	}
	
	public void addMeterToKVMap(Map<String, Object> kVMap) {
        for (Entry<String, Map<MetricType, Double>> entry : meterMapKV.entrySet()) {
        	String name = entry.getKey();
        	Map<MetricType, Double> typeMap = entry.getValue(); 

        	for (Entry<MetricType, Double> typeEntry : typeMap.entrySet()) {        	
        	    kVMap.put(name+ "_" + typeEntry.getKey().toString(), typeEntry.getValue()); 
        	}
        }
	}
	
	public void  countGangeMetric(Map<String, GaugeData> gaugeMap ){
		//count value of Gauge
		for(Entry<String, GaugeData> entry : gaugeMap.entrySet()){
			String taskMetricName = entry.getKey();
			String userDefName = taskMetricName.substring(taskMetricName.indexOf(":") + 1);
			Double value = entry.getValue().getValue();
			if(gaugeMapKV.containsKey(userDefName)){
				value = value + gaugeMapKV.get(userDefName);
			}
			gaugeMapKV.put(userDefName, value);
		}	
	}
	
	public void  countCounterMetric(Map<String, CounterData> counterMap){
		for (Entry<String, CounterData> entry : counterMap.entrySet()) {
			String taskMetricName = entry.getKey();
			String userDefName = taskMetricName.substring(taskMetricName.indexOf(":") + 1);
			Long value = entry.getValue().getValue();
			if(counterMapKV.containsKey(userDefName)){
				value = value + counterMapKV.get(userDefName);
			}
			counterMapKV.put(userDefName, value);
		}
	}
	
	public void  countHistogramMetric(Map<String, HistogramData> histogramMap){
        //only count: minValue, maxValue ,aveValue	
		for (Entry<String, HistogramData> entry : histogramMap.entrySet()) {
			String taskMetricName = entry.getKey();
			String userDefName = taskMetricName.substring(taskMetricName.indexOf(":") + 1);

		    double maxValue = entry.getValue().getMax();
		    double minValue = entry.getValue().getMin();
		    double meanValue = entry.getValue().getMean();
		    
		    Map<MetricType,Double> temMap = histogramMapKV.get(userDefName);
			if(temMap == null){
				temMap = new HashMap<MetricType, Double>();
				histogramMapKV.put(userDefName, temMap);
			}

			maxValue += (temMap.get(MetricType.max) == null ? 0l : temMap.get(MetricType.max));
			minValue += (temMap.get(MetricType.min) == null ? 0l : temMap.get(MetricType.min));
			meanValue += (temMap.get(MetricType.mean) == null ? 0.0 : temMap.get(MetricType.mean));

			temMap.put(MetricType.max, maxValue);
			temMap.put(MetricType.min, minValue);
			temMap.put(MetricType.mean, meanValue);		
		}
	}
	
	public void countTimerMetric(Map<String, TimerData> timerMap){
        //only count: mean time
		for(Entry<String, TimerData> entry:timerMap.entrySet()){
			String taskMetricName = entry.getKey();
			String userDefName = taskMetricName.substring(taskMetricName.indexOf(":") + 1);
		    double meanValue = (double)entry.getValue().getMean();

		    Map<MetricType, Double> temMap = timerMapKV.get(userDefName);
			if (temMap == null) {
			    temMap = new HashMap<MetricType, Double>();
			    timerMapKV.put(userDefName, temMap);
			}

			// It is possible that the timer function is not called in some tasks. So, for 
            // this case, the value should not be involved for following average time calculation.
            if (meanValue != 0.0) {
                Integer count = (countMap.get(userDefName) == null ? 0 : countMap.get(userDefName));
                count++;
                countMap.put(userDefName, count);
            }
			
			meanValue += (temMap.get(MetricType.mean) == null ? 0.0 : temMap.get(MetricType.mean));
			temMap.put(MetricType.mean, meanValue);  
		}
	}
	
	public void calcAvgTimer() {
		for (Entry<String, Map<MetricType, Double>> entry: timerMapKV.entrySet()) {
			String userDefName = entry.getKey();
			Map<MetricType, Double> valueMap = entry.getValue();
			Integer count = countMap.get(userDefName);
			if (count == null || count == 0) {
				LOG.warn("Name=" + userDefName + " is not found in countMap for timer, or count is zero");
				continue;
			}
			double meanValue = (valueMap.get(MetricType.mean))/count;
			valueMap.put(MetricType.mean, convertDurationFromNsToMs(meanValue));
		}
	}
	
	public void  countMeterMetric(Map<String, MeterData> meterMap){
        //only count: meanRate
		for(Entry<String, MeterData> entry:meterMap.entrySet()){
			String taskMetricName = entry.getKey();
			String userDefName = taskMetricName.substring(taskMetricName.indexOf(":")+1);
			
			Double meanRate = entry.getValue().getMeanRate();
			Map<MetricType, Double> temMap = meterMapKV.get(userDefName);
			if (temMap == null) {
			    temMap = new HashMap<MetricType, Double>();
			    meterMapKV.put(userDefName, temMap);
			}

			meanRate += (temMap.get(MetricType.mean) == null ? 0.0 : temMap.get(MetricType.mean));
			temMap.put(MetricType.mean, meanRate);
			meterMapKV.put(userDefName, temMap);
		}
	}
	
	public Map<String, Map<MetricType, Double>> getTimerKVMap() {
		return this.timerMapKV;
	}
	
	public void emptyCountMap() {
		countMap.clear();
	}
	
	private double convertDurationFromNsToMs(double duration) {
        return duration / TimeUnit.MILLISECONDS.toNanos(1);
    }
	
	public static MetricKVMsg getMetricKVMsg(String topologyId, StormClusterState clusterState) throws Exception {
        List<String> workerIds = clusterState.monitor_user_workers(topologyId);
        MetricKVMsg topologyMetricMsg = new MetricKVMsg();
        for (String workerId : workerIds) {
            UserDefMetricData useWorkDefMetric = clusterState.get_userDef_metric(topologyId, workerId);
            //add metric based on worker to useWorkDefMetric
            topologyMetricMsg.countGangeMetric(useWorkDefMetric.getGaugeDataMap());
            topologyMetricMsg.countCounterMetric(useWorkDefMetric.getCounterDataMap());
            topologyMetricMsg.countHistogramMetric(useWorkDefMetric.getHistogramDataMap());
            topologyMetricMsg.countTimerMetric(useWorkDefMetric.getTimerDataMap());
            topologyMetricMsg.countMeterMetric(useWorkDefMetric.getMeterDataMap());
        }
        topologyMetricMsg.calcAvgTimer();
        topologyMetricMsg.emptyCountMap();

        return topologyMetricMsg;
    }
	
	public List<UserDefMetric> convertToUserDefMetric() {
	    List<UserDefMetric> userDefMetrics = new ArrayList<UserDefMetric>();
	    
	    for (Entry<String, Double> entry : gaugeMapKV.entrySet()) {
	        userDefMetrics.add(new UserDefMetric("Gauge", entry.getKey(), entry.getValue()));
	    }
	    
	    for (Entry<String, Long> entry : counterMapKV.entrySet()) {
            userDefMetrics.add(new UserDefMetric("Counter", entry.getKey(), entry.getValue()));
        }
	    
	    for (Entry<String, Map<MetricType, Double>> entry : histogramMapKV.entrySet()) {
	        userDefMetrics.add(new UserDefMetric("Histogram", entry.getKey(), entry.getValue().get(MetricType.mean)));
	    }
	    
	    for (Entry<String, Map<MetricType, Double>> entry : timerMapKV.entrySet()) {
            userDefMetrics.add(new UserDefMetric("Timer", entry.getKey(), entry.getValue().get(MetricType.mean)));
        }
	    
	    for (Entry<String, Map<MetricType, Double>> entry : meterMapKV.entrySet()) {
            userDefMetrics.add(new UserDefMetric("Meter", entry.getKey(), entry.getValue().get(MetricType.mean)));
        }
	    
	    return userDefMetrics;
	}
}
