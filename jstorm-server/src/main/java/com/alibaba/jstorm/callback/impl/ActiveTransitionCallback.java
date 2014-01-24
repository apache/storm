package com.alibaba.jstorm.callback.impl;

import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.StatusType;

/**
 * Set the topology status as Active
 * 
 */
public class ActiveTransitionCallback extends BaseCallback {

	@Override
	public <T> Object execute(T... args) {

		return new StormStatus(StatusType.active);
	}

}
