package com.alibaba.jstorm.task.error;

import java.io.Serializable;

/**
 * Task error stored in Zk(/storm-zk-root/taskerrors/{topologyid}/{taskid})
 * 
 * @author yannian
 * 
 */
public class TaskError implements Serializable {

	private static final long serialVersionUID = 5028789764629555542L;
	private String error;
	private int timSecs;

	public TaskError(String error, int timSecs) {
		this.error = error;
		this.timSecs = timSecs;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public int getTimSecs() {
		return timSecs;
	}

	public void setTimSecs(int timSecs) {
		this.timSecs = timSecs;
	}

}
