package com.alibaba.jstorm.task.error;

import com.alibaba.jstorm.callback.RunnableCallback;

/**
 * The callback will be called, when task occur error It just call
 * TaskReportErrorAndDie
 * 
 * @author yannian
 * 
 */
public class TaskErrorRunable extends RunnableCallback {
	private TaskReportErrorAndDie report_error_and_die;

	public TaskErrorRunable(TaskReportErrorAndDie _report_error_and_die) {
		this.report_error_and_die = _report_error_and_die;
	}

	@Override
	public <T> Object execute(T... args) {
		Exception e = null;
		if (args != null && args.length > 0) {
			e = (Exception) args[0];
		}
		if (e != null) {
			report_error_and_die.report(e);
		}
		return null;
	}

}
