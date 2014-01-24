package com.alibaba.jstorm.callback;

import com.alibaba.jstorm.utils.JStormUtils;

/**
 * Killer callback
 * 
 * @author yannian
 * 
 */

public class AsyncLoopDefaultKill extends RunnableCallback {

	@Override
	public <T> Object execute(T... args) {
		Exception e = (Exception) args[0];
		JStormUtils.halt_process(1, "Async loop died!");
		return e;
	}

	@Override
	public void run() {
		JStormUtils.halt_process(1, "Async loop died!");
	}
}
