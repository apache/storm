package com.alibaba.jstorm.batch;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;

/**
 * Called before commit, after finish batch
 * 
 * @author zhongyan.feng
 */
public interface IPrepareCommit {

	/**
	 * Do prepare before commit
	 * 
	 * @param id
	 * @param collector
	 */
	void prepareCommit(BatchId id, BasicOutputCollector collector) throws FailedException;
}
