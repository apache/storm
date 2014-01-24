package com.alibaba.jstorm.stats.incval;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.utils.JStormUtils;

public class IncValMerger extends RunnableCallback {
	/**
	 * Merget List<Map<Object, Long>> to Map<Object, Long>
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Object execute(T... args) {
		Map<Object, Long> result = null;
		if (args != null && args.length > 0) {

			List<Map<Object, Long>> list = (List<Map<Object, Long>>) args[0];
			result = new HashMap<Object, Long>();

			for (Map<Object, Long> each : list) {

				for (Entry<Object, Long> e : each.entrySet()) {
					Object key = e.getKey();
					Long val = e.getValue();
					if (result.containsKey(key)) {
						val = (Long) JStormUtils.add(val, result.get(key));
					}
					result.put(key, val);
				}
			}
		}
		return result;
	}
}
