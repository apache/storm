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
package org.apache.storm.executor;

import org.apache.storm.Config;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;

public class ExecutorTransfer  {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorTransfer.class);

    private final WorkerState workerData;
    private final KryoTupleSerializer serializer;
    private final boolean isDebug;
    private final int producerBatchSz;
    private int remotesBatchSz = 0;
    private final ArrayList<JCQueue> localReceiveQueues; // [taksid]=queue
    private final ArrayList<JCQueue> outboundQueues; // [taksid]=queue, some entries can be null


    public ExecutorTransfer(WorkerState workerData, Map<String, Object> topoConf) {
        this.workerData = workerData;
        this.serializer = new KryoTupleSerializer(topoConf, workerData.getWorkerTopologyContext());
        this.isDebug = ObjectReader.getBoolean(topoConf.get(Config.TOPOLOGY_DEBUG), false);
        this.producerBatchSz = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_PRODUCER_BATCH_SIZE));
        this.localReceiveQueues = Utils.convertToArray(workerData.getShortExecutorReceiveQueueMap(), 0);
        this.outboundQueues = new ArrayList<JCQueue>(Collections.nCopies(localReceiveQueues.size(), null) );
    }


    public void transfer(AddressedTuple addressedTuple) throws InterruptedException {
        if (isDebug) {
            LOG.info("TRANSFERRING tuple {}", addressedTuple);
        }

        JCQueue localQueue = getLocalQueue(addressedTuple);
        if (localQueue!=null) {
            transferLocal(addressedTuple, localQueue);
        }  else  {
            transferRemote(addressedTuple);
            ++remotesBatchSz;
            if (remotesBatchSz >=producerBatchSz) {
                flushRemotes();
                remotesBatchSz =0;
            }
        }
    }

    // adds addressedTuple to destination Q is it is not full. else adds to overflow (if its not null)
    public boolean tryTransfer(AddressedTuple addressedTuple, Queue<AddressedTuple> overflow) {
        if (isDebug) {
            LOG.info("TRANSFERRING tuple {}", addressedTuple);
        }

        JCQueue localQueue = getLocalQueue(addressedTuple);
        if (localQueue!=null) {
            return tryTransferLocal(addressedTuple, localQueue, overflow);
        }  else  {
            if (remotesBatchSz >= producerBatchSz) {
                if ( !tryFlushRemotes() ) {
                    if (overflow != null) {
                        overflow.add(addressedTuple);
                    }
                    return false;
                }
                remotesBatchSz = 0;
            }
            if (tryTransferRemote(addressedTuple, overflow)) {
                ++remotesBatchSz;
                if (remotesBatchSz >= producerBatchSz) {
                    tryFlushRemotes();
                    remotesBatchSz = 0;
                }
                return true;
            }
            return false;
        }
    }

    private void transferRemote(AddressedTuple tuple) throws InterruptedException {
        workerData.transferRemote(tuple);
    }

    private boolean tryTransferRemote(AddressedTuple tuple, Queue<AddressedTuple> overflow) {
        return workerData.tryTransferRemote(tuple, overflow);
    }

    // flushes local and remote messages
    public void flush() throws InterruptedException {
        flushLocal();
        flushRemotes();
    }

    private void flushLocal() throws InterruptedException {
        for (int i = 0; i < outboundQueues.size(); i++) {
            JCQueue q = outboundQueues.get(i);
            if(q!=null)
                q.flush();
        }
    }

    private void flushRemotes() throws InterruptedException {
        workerData.flushRemotes();
    }

    private boolean tryFlushRemotes() {
        return workerData.tryFlushRemotes();
    }

    private JCQueue getLocalQueue(AddressedTuple tuple) {
        if (tuple.dest>=localReceiveQueues.size())
            return null;
        return localReceiveQueues.get(tuple.dest);
    }

    public void transferLocal(AddressedTuple tuple, JCQueue localQueue) throws InterruptedException {
        workerData.checkSerialize(serializer, tuple);
        localQueue.publish(tuple);
        outboundQueues.set(tuple.dest, localQueue);
    }

    // tries to add to localQueue, else adds to overflow. returns false if it cannot add to localQueue.
    public boolean tryTransferLocal(AddressedTuple tuple, JCQueue localQueue, Queue<AddressedTuple> overflow) {
        workerData.checkSerialize(serializer, tuple);
        if (localQueue.tryPublish(tuple)) {
            outboundQueues.set(tuple.dest, localQueue);
            return true;
        }
        if (overflow!=null) {
            overflow.add(tuple);
        }
        return false;
    }

}
