package com.alibaba.jstorm.task;

public class TaskStatus {
	// task is alive, and it will run BaseExecutor's run
	public static final byte RUN = 0;
	// task is alive, but it won't run BaseExecutor's run
	public static final byte PAUSE = 1;
	// task is shutdown
	public static final byte SHUTDOWN = 2;

	private volatile byte status = TaskStatus.RUN;

	public byte getStatus() {
		return status;
	}

	public void setStatus(byte status) {
		this.status = status;
	}

	public boolean isRun() {
		return status == TaskStatus.RUN;
	}

	public boolean isShutdown() {
		return status == TaskStatus.SHUTDOWN;
	}

	public boolean isPause() {
		return status == TaskStatus.PAUSE;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
