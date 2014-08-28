package com.alibaba.jstorm.batch;

import backtype.storm.topology.BasicOutputCollector;


public interface IPostCommit {
	/**
	 * Do after commit
	 * Don't care failure of postCommit
	 * 
	 * @param id
	 */
	void postCommit(BatchId id, BasicOutputCollector collector);
}
