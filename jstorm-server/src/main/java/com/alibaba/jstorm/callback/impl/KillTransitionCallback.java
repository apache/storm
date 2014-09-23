package com.alibaba.jstorm.callback.impl;

import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.StatusType;

/**
 * The action when nimbus receive killed command.
 * 
 * 1. change current topology status as killed 2. one TIMEOUT seconds later, do
 * remove action, which remove topology from ZK
 * 
 * @author Longda
 * 
 */
public class KillTransitionCallback extends DelayStatusTransitionCallback {

	public KillTransitionCallback(NimbusData data, String topologyid) {
		super(data, topologyid, null, StatusType.killed, StatusType.remove);
	}

}
