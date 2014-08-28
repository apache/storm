package com.alibaba.jstorm.batch;

import java.io.Serializable;

import backtype.storm.topology.FailedException;

/**
 * The less committer, the state is more stable.
 * Don't need to do 
 * 
 * @author zhongyan.feng
 * @version
 */
public interface ICommitter extends Serializable{
	/**
	 * begin to commit batchId's data, then return the commit result
	 * The commitResult will store into outside storage
	 * 
	 * if failed to commit, please throw FailedException
	 * 
	 * 
	 * 
	 * @param id
	 */
	byte[] commit(BatchId id) throws FailedException;
	
	/**
	 * begin to revert batchId's data
	 * 
	 * If current task fails to commit batchId, it won't call revert(batchId)
	 * If current task fails to revert batchId, JStorm won't call revert again.  
	 * 
	 * @param id
	 */
	void revert(BatchId id, byte[] commitResult);
}
