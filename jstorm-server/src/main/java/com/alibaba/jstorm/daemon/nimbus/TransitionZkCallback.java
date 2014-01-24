package com.alibaba.jstorm.daemon.nimbus;

import com.alibaba.jstorm.callback.RunnableCallback;

/**
 * This is ZK watch callback When supervisor Zk dir has been changed, it will
 * trigger this callback Set the status as monitor
 * 
 */
public class TransitionZkCallback extends RunnableCallback {

	private NimbusData data;
	private String topologyid;

	public TransitionZkCallback(NimbusData data, String topologyid) {
		this.data = data;
		this.topologyid = topologyid;
	}

	@Override
	public void run() {
		NimbusUtils.transition(data, topologyid, false, StatusType.monitor);
	}
}
