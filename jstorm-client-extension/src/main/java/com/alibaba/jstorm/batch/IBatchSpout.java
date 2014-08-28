package com.alibaba.jstorm.batch;

import java.io.Serializable;

import backtype.storm.topology.IBasicBolt;

public interface IBatchSpout extends IBasicBolt, ICommitter, Serializable {

	/**
	 * input's filed 0 is BatchId
	 * 
	 * execute only receive trigger message
	 * 
	 * do emitBatch operation in execute whose streamID is "batch/compute-stream"
	 */
	//void execute(Tuple input, IBasicOutputCollector collector);
    /**
	 * begin to ack batchId's data
	 * 
	 * return value will be stored in ZK, so sometimes don't need special action
	 * 
	 * @param id
	 */
	//byte[] commit(BatchId id) throws FailedException;
	
	/**
	 * begin to revert batchId's data
	 * 
	 * If current task fails to commit batchId, it won't call revert(batchId)
	 * If current task fails to revert batchId, JStorm won't call revert again.  
	 * 
	 * if not transaction, it can don't care revert
	 * 
	 * @param id
	 */
	//void revert(BatchId id, byte[] commitResult);
}
