package com.alibaba.jstorm.stats.keyAvg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.stats.StatFunction;
import com.alibaba.jstorm.utils.Pair;

public class KeyAvgMerge extends RunnableCallback {

	@SuppressWarnings("unchecked")
	@Override
	public <T> Object execute(T... args) {
		List<Map<Object, Pair<Long, Long>>> list = (List<Map<Object, Pair<Long, Long>>>) args[0];

		Map<Object, Pair<Long, Long>> result = new HashMap<Object, Pair<Long, Long>>();

		Map<Object, List<Pair<Long, Long>>> trans = new HashMap<Object, List<Pair<Long, Long>>>();

		for (Map<Object, Pair<Long, Long>> each : list) {

			for (Entry<Object, Pair<Long, Long>> e : each.entrySet()) {

				Object key = e.getKey();
				List<Pair<Long, Long>> val = trans.get(key);
				if (val == null) {
					val = new ArrayList<Pair<Long, Long>>();
				}
				val.add(e.getValue());
				trans.put(key, val);
			}
		}

		for (Entry<Object, List<Pair<Long, Long>>> e : trans.entrySet()) {
			result.put(e.getKey(), StatFunction.merge_keyed_avg(e.getValue()));
		}
		return result;
	}
}
