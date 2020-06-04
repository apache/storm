/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.executor;

import java.util.ArrayList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.storm.Config;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Every executor has an instance of this class
public class ExecutorTransfer {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorTransfer.class);

    private final WorkerState workerData;
    // one serializer per thread to avoid data corruption
    private final ThreadLocal<KryoTupleSerializer> threadLocalSerializer;
    private final boolean isDebug;
    private int indexingBase = 0;
    private ArrayList<JCQueue> localReceiveQueues; // [taskId-indexingBase] => queue : List of all recvQs local to this worker
    private AtomicReferenceArray<JCQueue> queuesToFlush;
    // [taskId-indexingBase] => queue, some entries can be null. : outbound Qs for this executor instance

    public ExecutorTransfer(WorkerState workerData, Map<String, Object> topoConf) {
        this.workerData = workerData;
        WorkerTopologyContext workerTopologyContext = workerData.getWorkerTopologyContext();
        this.threadLocalSerializer = ThreadLocal.withInitial(() -> new KryoTupleSerializer(topoConf, workerTopologyContext));
        this.isDebug = ObjectReader.getBoolean(topoConf.get(Config.TOPOLOGY_DEBUG), false);
    }

    // to be called after all Executor objects in the worker are created and before this object is used
    public void initLocalRecvQueues() {
        Integer minTaskId = workerData.getLocalReceiveQueues().keySet().stream().min(Integer::compareTo).get();
        this.localReceiveQueues = Utils.convertToArray(workerData.getLocalReceiveQueues(), minTaskId);
        this.indexingBase = minTaskId;
        this.queuesToFlush = new AtomicReferenceArray<JCQueue>(localReceiveQueues.size());
    }

    // adds addressedTuple to destination Q if it is not full. else adds to pendingEmits (if its not null)
    public boolean tryTransfer(AddressedTuple addressedTuple, Queue<AddressedTuple> pendingEmits) {
        if (isDebug) {
            LOG.info("TRANSFERRING tuple {}", addressedTuple);
        }

        JCQueue localQueue = getLocalQueue(addressedTuple);
        if (localQueue != null) {
            return tryTransferLocal(addressedTuple, localQueue, pendingEmits);
        }
        return workerData.tryTransferRemote(addressedTuple, pendingEmits, threadLocalSerializer.get());
    }


    // flushes local and remote messages
    public void flush() throws InterruptedException {
        flushLocal();
        workerData.flushRemotes();
    }

    private void flushLocal() throws InterruptedException {
        for (int i = 0; i < queuesToFlush.length(); i++) {
            JCQueue q = queuesToFlush.get(i);
            if (q != null) {
                q.flush();
                queuesToFlush.set(i, null);
            }
        }
    }


    public JCQueue getLocalQueue(AddressedTuple tuple) {
        if ((tuple.dest - indexingBase) >= localReceiveQueues.size()) {
            return null;
        }
        return localReceiveQueues.get(tuple.dest - indexingBase);
    }

    /**
     * Adds tuple to localQueue (if overflow is empty). If localQueue is full adds to pendingEmits instead. pendingEmits can be null.
     * Returns false if unable to add to localQueue.
     */
    public boolean tryTransferLocal(AddressedTuple tuple, JCQueue localQueue, Queue<AddressedTuple> pendingEmits) {
        workerData.checkSerialize(threadLocalSerializer.get(), tuple);
        if (pendingEmits != null) {
            if (pendingEmits.isEmpty() && localQueue.tryPublish(tuple)) {
                queuesToFlush.set(tuple.dest - indexingBase, localQueue);
                return true;
            } else {
                pendingEmits.add(tuple);
                return false;
            }
        } else {
            return localQueue.tryPublish(tuple);
        }
    }

}
