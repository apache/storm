package com.alibaba.jstorm.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.generated.GlobalStreamId;

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.common.stats.StatBuckets;
import com.alibaba.jstorm.common.stats.StaticsType;
import com.alibaba.jstorm.stats.rolling.RollingWindowSet;
import com.alibaba.jstorm.utils.EventSampler;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * both spout and bolt will use base statics
 * 
 * @author Longda/yannian
 * 
 */
public class CommonStatsRolling {

	private static final long serialVersionUID = -2145444660360278001L;

	protected Map<StaticsType, RollingWindowSet> staticTypeMap = new HashMap<StaticsType, RollingWindowSet>();

	// <StremId, EventSampler>
	protected Map<String, EventSampler> emittedSamplers;
	protected Map<String, EventSampler> sendTpsSamplers;

	// <componentId, <streamId, EventSampler>>
	protected Map<String, Map<String, EventSampler>> recvTpsSamplers;

	protected Map<String, Map<String, EventSampler>> processSamplers;

	// in order to improve performance, use two type of samplers
	// protected Map<String, Map<String, EventSampler>> ackedSamplers;
	// protected Map<String, Map<String, EventSampler>> failedSamplers;
	protected Map<String, Map<String, EventSampler>> boltAckedSamplers;
	protected Map<String, Map<String, EventSampler>> boltFailedSamplers;
	protected Map<String, EventSampler> spoutAckedSamplers;
	protected Map<String, EventSampler> spoutFailedSamplers;

	protected boolean enable = true;

	protected Integer rate;

	public CommonStatsRolling(Integer rate) {
		RollingWindowSet emitted = StatFunction
				.keyed_counter_rolling_window_set(
						StatFunction.NUM_STAT_BUCKETS, StatBuckets.STAT_BUCKETS);
		staticTypeMap.put(StaticsType.emitted, emitted);

		RollingWindowSet sendTps = StatFunction.keyed_avg_rolling_window_set(
				StatFunction.NUM_STAT_BUCKETS, StatBuckets.STAT_BUCKETS);
		staticTypeMap.put(StaticsType.send_tps, sendTps);

		RollingWindowSet recvTps = StatFunction.keyed_avg_rolling_window_set(
				StatFunction.NUM_STAT_BUCKETS, StatBuckets.STAT_BUCKETS);
		staticTypeMap.put(StaticsType.recv_tps, recvTps);

		RollingWindowSet acked = StatFunction.keyed_counter_rolling_window_set(
				StatFunction.NUM_STAT_BUCKETS, StatBuckets.STAT_BUCKETS);
		staticTypeMap.put(StaticsType.acked, acked);

		RollingWindowSet failed = StatFunction
				.keyed_counter_rolling_window_set(
						StatFunction.NUM_STAT_BUCKETS, StatBuckets.STAT_BUCKETS);
		staticTypeMap.put(StaticsType.failed, failed);

		RollingWindowSet process_latencies = StatFunction
				.keyed_avg_rolling_window_set(StatFunction.NUM_STAT_BUCKETS,
						StatBuckets.STAT_BUCKETS);

		staticTypeMap.put(StaticsType.process_latencies, process_latencies);

		emittedSamplers = new HashMap<String, EventSampler>();
		sendTpsSamplers = new HashMap<String, EventSampler>();
		recvTpsSamplers = new HashMap<String, Map<String, EventSampler>>();
		boltAckedSamplers = new HashMap<String, Map<String, EventSampler>>();
		boltFailedSamplers = new HashMap<String, Map<String, EventSampler>>();
		spoutAckedSamplers = new HashMap<String, EventSampler>();
		spoutFailedSamplers = new HashMap<String, EventSampler>();
		processSamplers = new HashMap<String, Map<String, EventSampler>>();

		this.rate = rate;
	}

	/**
	 * update statics
	 * 
	 * @param common
	 * @param path
	 * @param args
	 */
	public void update_task_stat(StaticsType type, Object... args) {

		RollingWindowSet statics = staticTypeMap.get(type);
		if (statics != null) {
			statics.update_rolling_window_set(args);
		}
	}

	public void send_tuple(String stream, int num_out_tasks) {
		if (enable == false) {
			return;
		}

		if (num_out_tasks <= 0) {
			return;
		}

		EventSampler emittedSampler = emittedSamplers.get(stream);
		if (emittedSampler == null) {
			emittedSampler = new EventSampler(rate);
			emittedSamplers.put(stream, emittedSampler);
		}

		Integer times = emittedSampler.timesCheck();
		if (times != null) {
			update_task_stat(StaticsType.emitted, stream, times * num_out_tasks);
		}

		EventSampler sendTpsSampler = sendTpsSamplers.get(stream);
		if (sendTpsSampler == null) {
			sendTpsSampler = new EventSampler(rate);
			sendTpsSamplers.put(stream, sendTpsSampler);
		}
		Integer send = sendTpsSampler.tpsCheck();

		if (send != null) {

			update_task_stat(StaticsType.send_tps, stream, send * num_out_tasks);

		}
	}

	public void recv_tuple(String component, String stream) {
		if (enable == false) {
			return;
		}

		Map<String, EventSampler> componentSamplers = recvTpsSamplers
				.get(component);
		if (componentSamplers == null) {
			componentSamplers = new HashMap<String, EventSampler>();
			recvTpsSamplers.put(component, componentSamplers);
		}

		EventSampler sampler = componentSamplers.get(stream);
		if (sampler == null) {
			sampler = new EventSampler(rate);
			componentSamplers.put(stream, sampler);
		}

		Integer recv = sampler.tpsCheck();

		if (recv != null) {
			GlobalStreamId key = new GlobalStreamId(component, stream);
			update_task_stat(StaticsType.recv_tps, key, recv);
		}
	}

	public void bolt_acked_tuple(String component, String stream,
			Long latency_ms) {

		if (enable == false) {
			return;
		}

		if (latency_ms == null) {
			return;
		}

		Map<String, EventSampler> componentSamplers = boltAckedSamplers
				.get(component);
		if (componentSamplers == null) {
			componentSamplers = new HashMap<String, EventSampler>();
			boltAckedSamplers.put(component, componentSamplers);
		}

		EventSampler sampler = componentSamplers.get(stream);
		if (sampler == null) {
			sampler = new EventSampler(rate);
			componentSamplers.put(stream, sampler);
		}

		Pair<Integer, Double> pair = sampler.avgCheck(latency_ms * 1000);
		if (pair == null) {
			return;
		}
		
		long avgLatency = (long)((double)pair.getSecond());
		GlobalStreamId key = new GlobalStreamId(component, stream);
		update_task_stat(StaticsType.acked, key, pair.getFirst());
		update_task_stat(StaticsType.process_latencies, key, avgLatency);
	}

	public void bolt_failed_tuple(String component, String stream) {
		if (enable == false) {
			return;
		}

		Map<String, EventSampler> componentSamplers = boltFailedSamplers
				.get(component);
		if (componentSamplers == null) {
			componentSamplers = new HashMap<String, EventSampler>();
			boltFailedSamplers.put(component, componentSamplers);
		}

		EventSampler sampler = componentSamplers.get(stream);
		if (sampler == null) {
			sampler = new EventSampler(rate);
			componentSamplers.put(stream, sampler);
		}

		Integer times = sampler.timesCheck();
		if (times == null) {
			return;
		}

		GlobalStreamId key = new GlobalStreamId(component, stream);
		update_task_stat(StaticsType.failed, key, times);
	}

	public void spout_acked_tuple(String stream, long st) {

		if (enable == false) {
			return;
		}

		if (st == 0) {
			return;
		}

		EventSampler sampler = spoutAckedSamplers.get(stream);
		if (sampler == null) {
			sampler = new EventSampler(rate);
			spoutAckedSamplers.put(stream, sampler);
		}

		

		long latency_ms = TimeUtils.time_delta_ms(st);
		Pair<Integer, Double> pair = sampler.avgCheck(latency_ms * 1000);
		if (pair == null) {
			return;
		}
		
		long avgLatency = (long)((double)pair.getSecond());
		GlobalStreamId key = new GlobalStreamId(Common.ACKER_COMPONENT_ID,
				stream);
		update_task_stat(StaticsType.acked, key, pair.getFirst());
		update_task_stat(StaticsType.process_latencies, key, avgLatency);
	}

	public void spout_failed_tuple(String stream) {
		if (enable == false) {
			return;
		}

		EventSampler sampler = spoutFailedSamplers.get(stream);
		if (sampler == null) {
			sampler = new EventSampler(rate);
			spoutFailedSamplers.put(stream, sampler);
		}

		Integer times = sampler.timesCheck();
		if (times == null) {
			return;
		}

		GlobalStreamId key = new GlobalStreamId(Common.ACKER_COMPONENT_ID,
				stream);
		update_task_stat(StaticsType.failed, key, times);
	}

	public CommonStatsData render_stats() {

		cleanup_stats(false);

		CommonStatsData ret = new CommonStatsData();

		for (Entry<StaticsType, RollingWindowSet> entry : staticTypeMap
				.entrySet()) {
			StaticsType type = entry.getKey();
			RollingWindowSet rws = entry.getValue();

			Map<Integer, Object> csData = rws.value_rolling_window_set();

			ret.put(type, csData);
		}

		return ret;
	}

	protected void cleanup_common_stats() {

		for (Entry<StaticsType, RollingWindowSet> entry : staticTypeMap
				.entrySet()) {
			RollingWindowSet rws = entry.getValue();
			rws.cleanup_rolling_window_set();
		}

	}

	public void cleanup_stats(boolean skipcommon) {
		if (skipcommon == false) {
			cleanup_common_stats();
		}

	}

	public RollingWindowSet get(StaticsType type) {
		return staticTypeMap.get(type);
	}

	public void put(StaticsType type, RollingWindowSet statics) {
		staticTypeMap.put(type, statics);
	}

	public Integer getRate() {
		return rate;
	}

	public void setRate(Integer rate) {
		this.rate = rate;
	}

}
