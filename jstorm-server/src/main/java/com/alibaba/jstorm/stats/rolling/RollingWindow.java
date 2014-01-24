package com.alibaba.jstorm.stats.rolling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.utils.TimeUtils;

public class RollingWindow {
	private RunnableCallback updater;
	private RunnableCallback merger;
	private RunnableCallback extractor;
	private Integer bucket_size_secs;
	private Integer num_buckets;
	// <TimeSecond, Map<streamId, staticsValue>>
	private Map<Integer, Map> buckets;

	public RunnableCallback getUpdater() {
		return updater;
	}

	public void setUpdater(RunnableCallback updater) {
		this.updater = updater;
	}

	public RunnableCallback getMerger() {
		return merger;
	}

	public void setMerger(RunnableCallback merger) {
		this.merger = merger;
	}

	public RunnableCallback getExtractor() {
		return extractor;
	}

	public void setExtractor(RunnableCallback extractor) {
		this.extractor = extractor;
	}

	public Integer getBucket_size_secs() {
		return bucket_size_secs;
	}

	public void setBucket_size_secs(Integer bucket_size_secs) {
		this.bucket_size_secs = bucket_size_secs;
	}

	public Integer getNum_buckets() {
		return num_buckets;
	}

	public void setNum_buckets(Integer num_buckets) {
		this.num_buckets = num_buckets;
	}

	public Map<Integer, Map> getBuckets() {
		return buckets;
	}

	public void setBuckets(Map<Integer, Map> buckets) {
		this.buckets = buckets;
	}

	public Integer curr_time_bucket(Integer time_secs) {
		return (Integer) (bucket_size_secs * (time_secs / bucket_size_secs));
	}

	// num_buckets == StatFunction.NUM_STAT_BUCKETS
	public static RollingWindow rolling_window(RunnableCallback updater,
			RunnableCallback merger, RunnableCallback extractor,
			Integer bucket_size_secs, Integer num_buckets) {

		RollingWindow rtn = new RollingWindow();
		rtn.setUpdater(updater);
		rtn.setMerger(merger);
		rtn.setExtractor(extractor);
		rtn.setBucket_size_secs(bucket_size_secs);
		rtn.setNum_buckets(num_buckets);

		rtn.setBuckets(new HashMap<Integer, Map>());

		return rtn;
	}

	/**
	 * 
	 * @param time_secs
	 * @param args
	 *            Object[0] -- key, Object[1] -- value
	 * @return
	 */
	public RollingWindow update_rolling_window(Integer time_secs, Object[] args) {
		synchronized (this) {
			Integer time_bucket = curr_time_bucket(time_secs);
			Map curr = buckets.get(time_bucket);

			UpdateParams p = new UpdateParams();
			p.setArgs(args);
			p.setCurr(curr);
			curr = (Map) updater.execute((Object) p);

			buckets.put(time_bucket, curr);

			return this;
		}
	}

	/**
	 * 
	 * @return <streamId, counter/statics>
	 */
	public Object value_rolling_window() {
		synchronized (this) {
			// <TimeSecond, Map<streamId, staticsValue>> buckets
			List<Map> values = new ArrayList<Map>();
			for (Entry<Integer, Map> entry : buckets.entrySet()) {
				values.add(entry.getValue());
			}

			// <streamId, counter> -- result
			Object result = merger.execute(values);
			return extractor.execute(result);
		}

	}

	public RollingWindow cleanup_rolling_window(int cutoff) {
		synchronized (this) {
			List<Integer> toremove = new ArrayList<Integer>();
			for (Entry<Integer, Map> entry : buckets.entrySet()) {
				Integer key = entry.getKey();
				if (key < cutoff) {
					toremove.add(key);
				}
			}

			for (Integer i : toremove) {
				buckets.remove(i);
			}
			return this;
		}
	}

	/**
	 * clean old data in the buckets before (now - bucket_size_secs *
	 * num_buckets)
	 * 
	 * @param cutoff
	 * @return
	 */
	public RollingWindow cleanup_rolling_window() {
		int cutoff = TimeUtils.current_time_secs() - rolling_window_size();
		return cleanup_rolling_window(cutoff);
	}

	public int rolling_window_size() {
		return bucket_size_secs * num_buckets;
	}

}
