/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.task;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.daemon.worker.timer.TaskBatchCheckTrigger;
import com.alibaba.jstorm.daemon.worker.timer.TaskBatchFlushTrigger;
import com.alibaba.jstorm.utils.EventSampler;
import com.alibaba.jstorm.utils.Pair;

import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.ITupleExt;
import backtype.storm.tuple.TupleExt;

/**
 * Batch Tuples, then send out
 * 
 * @author basti.lj
 * 
 */
public class TaskBatchTransfer extends TaskTransfer {

    private static Logger LOG = LoggerFactory.getLogger(TaskBatchTransfer.class);
    protected static final double BATCH_SIZE_THRESHOLD = 2.0;
    protected static final int BATCH_FLUSH_INTERVAL_MS = 5;
    protected static final int BATCH_CHECK_INTERVAL_S = 3600;
    protected static final int BATCH_EVENT_SAMPLER_INTERVAL_S = 4 * 240;
    
    private Map<Integer, BatchTuple> batchMap;
    private final int maxBatchSize;
    private int batchSize;
    private Object lock = new Object();
    private EventSampler eventSampler = null;

    public TaskBatchTransfer(Task task, String taskName, KryoTupleSerializer serializer, TaskStatus taskStatus, WorkerData workerData) {
        super(task, taskName, serializer, taskStatus, workerData);

        batchMap = new HashMap<Integer, BatchTuple>();
        maxBatchSize = ConfigExtension.getTaskMsgBatchSize(workerData.getStormConf());
        
        
        TaskBatchFlushTrigger batchFlushTrigger = new TaskBatchFlushTrigger(BATCH_FLUSH_INTERVAL_MS, taskName, this);
        batchFlushTrigger.register(TimeUnit.MILLISECONDS);
        
        TaskBatchCheckTrigger batchCheckTrigger = new TaskBatchCheckTrigger(BATCH_CHECK_INTERVAL_S, taskName, this);
        batchCheckTrigger.register();
        
        startCheck();
    }
    
    public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		LOG.info(taskName + " set batch size as " + batchSize);
	}

    @Override
    protected AsyncLoopThread setupSerializeThread() {
        return new AsyncLoopThread(new TransferBatchRunnable());
    }
    
    public void startCheck() {
    	eventSampler = new EventSampler(BATCH_EVENT_SAMPLER_INTERVAL_S);
    	setBatchSize(maxBatchSize);
    	LOG.info("Start check batch size, task of  " + taskName);
    }
    
    public void stopCheck() {
    	eventSampler = null;
    	LOG.info("Stop check batch size, task of  " + taskName);
    }

	@Override
	public void push(int taskId, TupleExt tuple) {
		synchronized (lock) {
			BatchTuple batch = getBatchTuple(taskId);

			batch.addToBatch(tuple);
			if (batch.isBatchFull()) {
				serializeQueue.publish(batch);
				batchMap.put(taskId, new BatchTuple(taskId, batchSize));
			}
		}

	}

    public void flush() {
    	Map<Integer, BatchTuple> oldBatchMap = null;
        synchronized (lock) {
            oldBatchMap = batchMap;
            batchMap = new HashMap<Integer, BatchTuple>();
        }
        
        for (Entry<Integer, BatchTuple> entry : oldBatchMap.entrySet()) {
            BatchTuple batch = entry.getValue();
            if (batch != null && batch.currBatchSize() > 0) {
            	serializeQueue.publish(batch);
            }
        }
    }


    private BatchTuple getBatchTuple(int targetTaskId) {
        BatchTuple ret = batchMap.get(targetTaskId);
        if (ret == null) {
            ret = new BatchTuple(targetTaskId, batchSize);
            batchMap.put(targetTaskId, ret);
        }
        return ret;
    }


    protected class TransferBatchRunnable extends TransferRunnable {

        public byte[] serialize(ITupleExt tuple) {
    		BatchTuple batchTuple = (BatchTuple)tuple;
    		if (eventSampler != null) {
    			Pair<Integer, Double> result = eventSampler.avgCheck(batchTuple.currBatchSize());
    			if (result != null) {
    				Double avgBatchSize = result.getSecond();
    				LOG.info(taskName + " batch average size is " + avgBatchSize);
    				if (avgBatchSize < BATCH_SIZE_THRESHOLD) {
    					LOG.info("Due to average size is small, so directly reset batch size as 1");
    					// set the batch size as 1
    					// transfer can directly send tuple, don't need wait flush interval
    					setBatchSize(1);
    				}
    				stopCheck();
    			}
    			
    		}
        	return serializer.serializeBatch(batchTuple);
        }
    }
}
