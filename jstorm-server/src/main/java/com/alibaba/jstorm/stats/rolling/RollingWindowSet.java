package com.alibaba.jstorm.stats.rolling;

import java.util.HashMap;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.utils.TimeUtils;

public class RollingWindowSet {
	private RunnableCallback updater;
	private RunnableCallback extractor;
	private RollingWindow[] windows;
	// all_time store the all_time result
	private Object all_time;

	public RunnableCallback getUpdater() {
		return updater;
	}

	public void setUpdater(RunnableCallback updater) {
		this.updater = updater;
	}

	public RunnableCallback getExtractor() {
		return extractor;
	}

	public void setExtractor(RunnableCallback extractor) {
		this.extractor = extractor;
	}

	public RollingWindow[] getWindows() {
		return windows;
	}

	public void setWindows(RollingWindow[] windows) {
		this.windows = windows;
	}

	public Object getAll_time() {
		return all_time;
	}

	public void setAll_time(Object all_time) {
		this.all_time = all_time;
	}

	public static RollingWindowSet rolling_window_set(RunnableCallback updater,
			RunnableCallback merger, RunnableCallback extractor,
			Integer num_buckets, Integer[] bucket_size) {

		RollingWindowSet rtn = new RollingWindowSet();

		rtn.setUpdater(updater);

		rtn.setExtractor(extractor);

		rtn.setWindows(new RollingWindow[bucket_size.length]);
		int bSize = bucket_size.length;
		for (int i = 0; i < bSize; i++) {
			rtn.getWindows()[i] = RollingWindow.rolling_window(updater, merger,
					extractor, bucket_size[i], num_buckets);
		}
		rtn.setAll_time(null);
		return rtn;
	}

	/**
	 * 
	 * @param args
	 *            Object[0] -- key, Object[1] -- value
	 */
	public void update_rolling_window_set(Object[] args) {
		synchronized (this) {
			int now = TimeUtils.current_time_secs();
			int winSize = windows.length;
			for (int i = 0; i < winSize; i++) {
				windows[i] = windows[i].update_rolling_window(now, args);
			}

			UpdateParams p = new UpdateParams();
			p.setArgs(args);
			p.setCurr(getAll_time());

			setAll_time(updater.execute(p));
		}
	}

	public RollingWindowSet cleanup_rolling_window_set() {
		synchronized (this) {
			for (int i = 0; i < windows.length; i++) {
				windows[i] = windows[i].cleanup_rolling_window();
			}
			return this;
		}
	}

	// Key -- 0 -- all time
	/**
	 * Keys: 0 -- all time summary { 30 * 20, 540 * 20, 4320 * 20 } seconds
	 * summary
	 * 
	 * @return
	 */
	public HashMap<Integer, Object> value_rolling_window_set() {

		HashMap<Integer, Object> rtn = new HashMap<Integer, Object>();

		synchronized (this) {
			int wSize = windows.length;
			for (int i = 0; i < wSize; i++) {
				int size = windows[i].rolling_window_size();
				// <streamId, counter/statics> -- obj
				Object obj = windows[i].value_rolling_window();

				rtn.put(size, obj);
			}

			Object result = extractor.execute(all_time);

			rtn.put(0, result);
			return rtn;
		}
	}
}
