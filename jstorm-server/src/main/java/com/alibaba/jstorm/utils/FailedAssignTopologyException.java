package com.alibaba.jstorm.utils;

public class FailedAssignTopologyException extends RuntimeException {

	private static final long serialVersionUID = 6921061096898899476L;

	public FailedAssignTopologyException() {
		super();
	}

	public FailedAssignTopologyException(String msg) {
		super(msg);
	}

	public FailedAssignTopologyException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public FailedAssignTopologyException(Throwable cause) {
		super(cause);
	}
}
