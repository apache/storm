package com.alibaba.jstorm.task.error;

/**
 * task report error interface
 * 
 * @author yannian
 * 
 */
public interface ITaskReportErr {
	public void report(Throwable error);
}
