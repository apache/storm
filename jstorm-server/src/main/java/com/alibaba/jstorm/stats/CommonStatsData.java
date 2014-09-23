package com.alibaba.jstorm.stats;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.TaskStats;

import com.alibaba.jstorm.common.stats.StatBuckets;
import com.alibaba.jstorm.common.stats.StaticsType;

/**
 * Common stats data
 * 
 * @author yannian/Longda
 * 
 */
public class CommonStatsData implements Serializable {

	private static final long serialVersionUID = -2811225938044543165L;

	protected Map<StaticsType, Map<Integer, Object>> staticsMap;
	protected int rate = StatFunction.NUM_STAT_BUCKETS;
	
	public static final long LATENCY_MS_RATIO = 1000;
	
	public static final Integer ALL_TIME_KEY = new Integer(0);

	public CommonStatsData() {
		staticsMap = new HashMap<StaticsType, Map<Integer, Object>>();

		// <window, Map<Stream, counter>>
		HashMap<Integer, Object> emitted = new HashMap<Integer, Object>();
		HashMap<Integer, Object> sendTps = new HashMap<Integer, Object>();
		HashMap<Integer, Object> recvTps = new HashMap<Integer, Object>();
		HashMap<Integer, Object> acked = new HashMap<Integer, Object>();
		HashMap<Integer, Object> failed = new HashMap<Integer, Object>();
		HashMap<Integer, Object> processLatencies = new HashMap<Integer, Object>();

		staticsMap.put(StaticsType.emitted, emitted);
		staticsMap.put(StaticsType.send_tps, sendTps);
		staticsMap.put(StaticsType.recv_tps, recvTps);
		staticsMap.put(StaticsType.acked, acked);
		staticsMap.put(StaticsType.failed, failed);
		staticsMap.put(StaticsType.process_latencies, processLatencies);

	}

	public Map<Integer, Object> get(StaticsType type) {
		return staticsMap.get(type);
	}

	public void put(StaticsType type, Map<Integer, Object> value) {
		staticsMap.put(type, value);
	}

	public int getRate() {
		return rate;
	}

	public void setRate(int rate) {
		this.rate = rate;
	}

	@Override
	public boolean equals(Object assignment) {
		if ((assignment instanceof CommonStatsData) == false) {
			return false;
		}

		CommonStatsData otherData = (CommonStatsData) assignment;

		for (Entry<StaticsType, Map<Integer, Object>> entry : staticsMap
				.entrySet()) {
			StaticsType type = entry.getKey();
			Map<Integer, Object> value = entry.getValue();

			Map<Integer, Object> otherValue = otherData.get(type);

			if (value.equals(otherValue) == false) {
				return false;
			}
		}

		return true;
	}

	@Override
	public int hashCode() {
		int ret = 0;
		for (Entry<StaticsType, Map<Integer, Object>> entry : staticsMap
				.entrySet()) {
			StaticsType type = entry.getKey();
			Map<Integer, Object> value = entry.getValue();

			ret += value.hashCode();
		}
		return ret;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

	public <K, V> Map<String, Map<K, V>> convertKey(
			Map<Integer, Object> statics, K keySample, V valueSample) {

		Map<String, Map<K, V>> ret = new HashMap<String, Map<K, V>>();

		for (Entry<Integer, Object> times : statics.entrySet()) {

			Map<K, V> val = (Map<K, V>) times.getValue();

			Integer window = times.getKey();

			String key = StatBuckets.parseTimeKey(window);

			ret.put(key, val);
		}

		return ret;
	}
	

	/**
	 * Get emmitted statics
	 * 
	 * @return <window, Map<stream, counter>>
	 */
	public Map<String, Map<String, Long>> get_emitted() {
		Map<Integer, Object> statics = staticsMap.get(StaticsType.emitted);

		return convertKey(statics, String.valueOf(""), Long.valueOf(0));
	}

	public Map<String, Map<String, Double>> get_send_tps() {
		Map<Integer, Object> statics = staticsMap.get(StaticsType.send_tps);

		return convertKey(statics, String.valueOf(""), Double.valueOf(0));
	}

	public Map<String, Map<GlobalStreamId, Double>> get_recv_tps() {
		Map<Integer, Object> statics = staticsMap.get(StaticsType.recv_tps);

		GlobalStreamId streamIdSample = new GlobalStreamId("", "");
		return convertKey(statics, streamIdSample, Double.valueOf(0));
	}

	public Map<String, Map<GlobalStreamId, Long>> get_acked() {
		Map<Integer, Object> statics = staticsMap.get(StaticsType.acked);

		GlobalStreamId streamIdSample = new GlobalStreamId("", "");
		return convertKey(statics, streamIdSample, Long.valueOf(0));
	}

	public Map<String, Map<GlobalStreamId, Long>> get_failed() {
		Map<Integer, Object> statics = staticsMap.get(StaticsType.failed);

		GlobalStreamId streamIdSample = new GlobalStreamId("", "");
		return convertKey(statics, streamIdSample, Long.valueOf(0));
	}

	public Map<String, Map<GlobalStreamId, Double>> get_process_latencie() {
		Map<String, Map<GlobalStreamId, Double>> ret = 
				new HashMap<String, Map<GlobalStreamId,Double>>();
		
		Map<Integer, Object> statics = staticsMap
				.get(StaticsType.process_latencies);

		GlobalStreamId streamIdSample = new GlobalStreamId("", "");
		
		Map<String, Map<GlobalStreamId, Double>> raw = 
				convertKey(statics, streamIdSample, Double.valueOf(0));
		
		for (Entry<String, Map<GlobalStreamId, Double>> windowEntry : raw.entrySet()) {
			String windowStr = windowEntry.getKey();
			Map<GlobalStreamId, Double> oldStreamMap = windowEntry.getValue();
			
			Map<GlobalStreamId, Double> newStreamMap = new HashMap<GlobalStreamId, Double>();
			
			for (Entry<GlobalStreamId, Double> entry: oldStreamMap.entrySet()) {
				GlobalStreamId key = entry.getKey();
				Double         value = entry.getValue();
				
				if (value == null) {
					newStreamMap.put(key, Double.valueOf(0));
				}else {
					newStreamMap.put(key, value/LATENCY_MS_RATIO);
				}
				
			}
			
			ret.put(windowStr, newStreamMap);
		}
		
		return ret;
	}
	
	/*
	 * Get total statics of all-time
	 */
	public Long get_total_emitted() {
		Long ret = new Long(0);
		
		Map<String, Long> allTimeEmitted = get_emitted().get(StatBuckets.ALL_WINDOW_STR);
		for (Entry<String, Long> entry : allTimeEmitted.entrySet()) {
			ret += entry.getValue();
		}		
		
		return ret;
	}
	
	public Double get_total_send_tps() {
		Double ret = new Double(0);
		
		Map<String, Double> allTimeSendTps = get_send_tps().get(StatBuckets.ALL_WINDOW_STR);
		for (Entry<String, Double> entry : allTimeSendTps.entrySet()) {
			ret += entry.getValue();
		}
		
		return ret;
	}
	
	public Double get_total_recv_tps() {
		Double ret = new Double(0);
		
		Map<GlobalStreamId, Double> allTimeRecvTps = get_recv_tps().get(StatBuckets.ALL_WINDOW_STR);
		for (Entry<GlobalStreamId, Double> entry : allTimeRecvTps.entrySet()) {
			ret += entry.getValue();
		}
		
		return ret;
	}
	
	public Long get_total_failed() {
		Long ret = new Long(0);
		
		Map<GlobalStreamId, Long> allTimeFailed = get_failed().get(StatBuckets.ALL_WINDOW_STR);
		for (Entry<GlobalStreamId, Long> entry : allTimeFailed.entrySet()) {
			ret += entry.getValue();
		}
		
		return ret;
	}
	
	public Double get_avg_latency() {
		Double ret = new Double(0);
		int i = 0;
		
		Map<GlobalStreamId, Double> allAvglatency = get_process_latencie().get(StatBuckets.ALL_WINDOW_STR);
		for (Entry<GlobalStreamId, Double> entry : allAvglatency.entrySet()) {
			ret += entry.getValue();
			i++;
		}
		return ret;
	}

	public TaskStats getTaskStats() {
		TaskStats taskStats = new TaskStats();

		taskStats.set_emitted(get_emitted());
		taskStats.set_send_tps(get_send_tps());
		taskStats.set_recv_tps(get_recv_tps());
		taskStats.set_acked(get_acked());
		taskStats.set_failed(get_failed());
		taskStats.set_process_ms_avg(get_process_latencie());

		return taskStats;
	}
}
