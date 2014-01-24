package com.alibaba.jstorm.daemon.worker;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.utils.JStormUtils;

public class WorkerHaltRunable extends RunnableCallback {

	@Override
	public void run() {
		JStormUtils.halt_process(1, "Task died");
	}

}
