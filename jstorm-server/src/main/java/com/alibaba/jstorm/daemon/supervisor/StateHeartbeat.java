package com.alibaba.jstorm.daemon.supervisor;

import com.alibaba.jstorm.daemon.worker.State;
import com.alibaba.jstorm.daemon.worker.WorkerHeartbeat;

/**
 * Worker's state and Hearbeat
 * 
 * @author Xin.Zhou
 */
public class StateHeartbeat {
	private State state;
	private WorkerHeartbeat hb;

	public StateHeartbeat(State state, WorkerHeartbeat hb) {
		this.state = state;
		this.hb = hb;
	}

	public State getState() {
		return this.state;
	}

	public WorkerHeartbeat getHeartbeat() {
		return this.hb;
	}

}
