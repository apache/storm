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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.WorkerData;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.TupleExt;
import backtype.storm.utils.DisruptorQueue;

/**
 * Batch Tuples, then send out
 * 
 * @author basti.lj
 * 
 */
public class TaskBatchTransfer extends TaskTransfer {

    private static Logger LOG = LoggerFactory
            .getLogger(TaskBatchTransfer.class);

    private Map<Integer, BatchTuple> batchMap;
    private int batchSize;
    private Object lock = new Object();

    public TaskBatchTransfer(Task task, String taskName,
            KryoTupleSerializer serializer, TaskStatus taskStatus,
            WorkerData workerData) {
        super(task, taskName, serializer, taskStatus, workerData);

        batchMap = new HashMap<Integer, BatchTuple>();
        batchSize =
                ConfigExtension.getTaskMsgBatchSize(workerData.getStormConf());
    }

    @Override
    protected AsyncLoopThread setupSerializeThread() {
        return new AsyncLoopThread(new TransferBatchRunnable());
    }

    @Override
    public void transfer(TupleExt tuple) {
        int targetTaskid = tuple.getTargetTaskId();
        synchronized (lock) {
            BatchTuple batch = getBatchTuple(targetTaskid);

            batch.addToBatch(tuple);
            if (batch.isBatchFull()) {
                pushToQueue(targetTaskid, batch);
            }
        }
    }

    public void flush() {
        synchronized (lock) {
            for (Entry<Integer, BatchTuple> entry : batchMap.entrySet()) {
                int taskId = entry.getKey();
                BatchTuple batch = entry.getValue();
                if (batch != null && batch.currBatchSize() > 0) {
                    pushToQueue(taskId, batch);
                }
            }
        }
    }

    private void pushToQueue(int targetTaskid, BatchTuple batch) {
        DisruptorQueue exeQueue = innerTaskTransfer.get(targetTaskid);
        if (exeQueue != null) {
            exeQueue.publish(batch);
        } else {
            serializeQueue.publish(batch);
        }
        resetBatchTuple(targetTaskid);
    }

    private BatchTuple getBatchTuple(int targetTaskId) {
        BatchTuple ret = batchMap.get(targetTaskId);
        if (ret == null) {
            ret = new BatchTuple(targetTaskId, batchSize);
            batchMap.put(targetTaskId, ret);
        }
        return ret;
    }

    private void resetBatchTuple(int targetTaskId) {
        batchMap.put(targetTaskId, null);
    }

    protected class TransferBatchRunnable extends TransferRunnable {
        @Override
        public void onEvent(Object event, long sequence, boolean endOfBatch)
                throws Exception {

            if (event == null) {
                return;
            }

            long start = System.currentTimeMillis();
            try {
                BatchTuple tuple = (BatchTuple) event;
                int taskid = tuple.getTargetTaskId();
                byte[] tupleMessage = serializer.serializeBatch(tuple);
                TaskMessage taskMessage = new TaskMessage(taskid, tupleMessage);
                IConnection conn = getConnection(taskid);
                if (conn != null)
                    conn.send(taskMessage);
            } finally {
                long end = System.currentTimeMillis();
                timer.update(end - start);
            }

        }
    }
}
