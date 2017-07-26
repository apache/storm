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
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

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
        this.localReceiveQueues = Utils.convertToArray(workerData.getShortExecutorReceiveQueueMap());
        this.outboundQueues = new ArrayList<JCQueue>(Collections.nCopies(localReceiveQueues.size(), null) );
    }


    public void transfer(int task, Tuple tuple) throws InterruptedException {
        AddressedTuple addressedTuple = new AddressedTuple(task, tuple);
        if (isDebug) {
            LOG.info("TRANSFERRING tuple {}", addressedTuple);
        }

        boolean isLocal = transferLocal(addressedTuple);
        if (!isLocal) {
            transferRemote(addressedTuple);
            ++remotesBatchSz;
            if(remotesBatchSz >=producerBatchSz) {
                flushRemotes();
                remotesBatchSz =0;
            }
        }
    }

    private void transferRemote(AddressedTuple tuple) throws InterruptedException {
        workerData.transferRemote(tuple);
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

    public boolean transferLocal(AddressedTuple tuple) throws InterruptedException {
        if(tuple.dest>=localReceiveQueues.size())
            return false;
        JCQueue queue = localReceiveQueues.get(tuple.dest);
        if (queue==null)
            return false;
        workerData.checkSerialize(serializer, tuple);
        queue.publish(tuple);
        outboundQueues.set(tuple.dest, queue);
        return true;
    }

}
