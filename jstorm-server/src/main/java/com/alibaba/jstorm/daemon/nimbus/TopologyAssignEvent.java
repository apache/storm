package com.alibaba.jstorm.daemon.nimbus;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.alibaba.jstorm.cluster.StormStatus;

public class TopologyAssignEvent {

	// unit is minutes
	private static final int DEFAULT_WAIT_TIME = 2;
	private String topologyId;
	private String topologyName; // if this field has been set, it is create
	private String group;
									// topology
	private boolean isScratch;
	private StormStatus oldStatus; // if this field has been set, it is
									// rebalance
	private CountDownLatch latch = new CountDownLatch(1);
	private boolean isSuccess = false;
	private String errorMsg;

	public String getTopologyId() {
		return topologyId;
	}

	public void setTopologyId(String topologyId) {
		this.topologyId = topologyId;
	}

	public boolean isScratch() {
		return isScratch;
	}

	public void setScratch(boolean isScratch) {
		this.isScratch = isScratch;
	}

	public StormStatus getOldStatus() {
		return oldStatus;
	}

	public void setOldStatus(StormStatus oldStatus) {
		this.oldStatus = oldStatus;
	}

	public String getTopologyName() {
		return topologyName;
	}

	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}

	public boolean waitFinish() {
		try {
			latch.await(DEFAULT_WAIT_TIME, TimeUnit.MINUTES);
		} catch (InterruptedException e) {

		}
		return isSuccess;
	}

	public boolean isFinish() {
		return latch.getCount() == 0;
	}

	public void done() {
		isSuccess = true;
		latch.countDown();
	}

	public void fail(String errorMsg) {
		isSuccess = false;
		this.errorMsg = errorMsg;
		latch.countDown();
	}

	public String getErrorMsg() {
		return errorMsg;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

}
