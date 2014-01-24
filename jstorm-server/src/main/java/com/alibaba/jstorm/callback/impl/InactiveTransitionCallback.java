package com.alibaba.jstorm.callback.impl;

import com.alibaba.jstorm.callback.BaseCallback;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.StatusType;

/**
 * 
 * set Topology status as inactive
 * 
 * Here just return inactive status Later, it will set inactive status to ZK
 */
public class InactiveTransitionCallback extends BaseCallback {

	@Override
	public <T> Object execute(T... args) {

		return new StormStatus(StatusType.inactive);
	}

}
