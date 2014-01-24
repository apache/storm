package com.alibaba.jstorm.stats.incval;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.jstorm.callback.RunnableCallback;

public class IncValExtractor extends RunnableCallback {

	@SuppressWarnings("unchecked")
	@Override
	public <T> Object execute(T... args) {
		Map<Object, Long> v = null;
		if (args != null && args.length > 0) {
			v = (Map<Object, Long>) args[0];
		}
		if (v == null) {
			v = new HashMap<Object, Long>();
		}
		return v;
	}
}
