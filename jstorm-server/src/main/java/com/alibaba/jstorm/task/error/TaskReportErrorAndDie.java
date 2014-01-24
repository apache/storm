package com.alibaba.jstorm.task.error;

import com.alibaba.jstorm.daemon.worker.WorkerHaltRunable;

/**
 * Task report error to ZK and halt the process
 * 
 * @author yannian
 * 
 */
public class TaskReportErrorAndDie implements ITaskReportErr {
	private ITaskReportErr reporterror;
	private WorkerHaltRunable haltfn;

	public TaskReportErrorAndDie(ITaskReportErr _reporterror,
			WorkerHaltRunable _haltfn) {
		this.reporterror = _reporterror;
		this.haltfn = _haltfn;
	}

	@Override
	public void report(Throwable error) {
		this.reporterror.report(error);
		this.haltfn.run();
	}
}
