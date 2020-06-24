/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.daemon.worker;

import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.serialization.ITupleSerializer;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.TransferDrainer;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.Utils.SmartThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Transfers messages destined to other workers
public class WorkerTransfer implements JCQueue.Consumer {
    static final Logger LOG = LoggerFactory.getLogger(WorkerTransfer.class);

    private final TransferDrainer drainer;
    private final WorkerState workerState;

    private final IWaitStrategy backPressureWaitStrategy;

    private JCQueue transferQueue; // [remoteTaskId] -> JCQueue. Some entries maybe null (if no emits to those tasksIds from this worker)

    private final AtomicBoolean[] remoteBackPressureStatus; // [[remoteTaskId] -> true/false : indicates if remote task is under BP.

    public WorkerTransfer(WorkerState workerState, Map<String, Object> topologyConf, int maxTaskIdInTopo) {
        this.workerState = workerState;
        this.backPressureWaitStrategy = IWaitStrategy.createBackPressureWaitStrategy(topologyConf);
        this.drainer = new TransferDrainer();
        this.remoteBackPressureStatus = new AtomicBoolean[maxTaskIdInTopo + 1];
        for (int i = 0; i < remoteBackPressureStatus.length; i++) {
            remoteBackPressureStatus[i] = new AtomicBoolean(false);
        }

        Integer xferQueueSz = ObjectReader.getInt(topologyConf.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE));
        Integer xferBatchSz = ObjectReader.getInt(topologyConf.get(Config.TOPOLOGY_TRANSFER_BATCH_SIZE));
        if (xferBatchSz > xferQueueSz / 2) {
            throw new IllegalArgumentException(Config.TOPOLOGY_TRANSFER_BATCH_SIZE + ":" + xferBatchSz + " must be no more than half of "
                                               + Config.TOPOLOGY_TRANSFER_BUFFER_SIZE + ":" + xferQueueSz);
        }

        this.transferQueue = new JCQueue("worker-transfer-queue", "worker-transfer-queue",
            xferQueueSz, 0, xferBatchSz, backPressureWaitStrategy,
            workerState.getTopologyId(), Constants.SYSTEM_COMPONENT_ID, Collections.singletonList(-1), workerState.getPort(),
            workerState.getMetricRegistry());
    }

    public JCQueue getTransferQueue() {
        return transferQueue;
    }

    AtomicBoolean[] getRemoteBackPressureStatus() {
        return remoteBackPressureStatus;
    }

    public SmartThread makeTransferThread() {
        return Utils.asyncLoop(() -> {
            if (transferQueue.consume(this) == 0) {
                return 1L;
            }
            return 0L;
        });
    }

    @Override
    public void accept(Object tuple) {
        TaskMessage tm = (TaskMessage) tuple;
        drainer.add(tm);
    }

    @Override
    public void flush() throws InterruptedException {
        ReentrantReadWriteLock.ReadLock readLock = workerState.endpointSocketLock.readLock();
        try {
            readLock.lock();
            drainer.send(workerState.cachedTaskToNodePort.get(), workerState.cachedNodeToPortSocket.get());
        } finally {
            readLock.unlock();
        }
        drainer.clear();
    }

    /* Not a Blocking call. If cannot emit, will add 'tuple' to 'pendingEmits' and return 'false'. 'pendingEmits' can be null */
    public boolean tryTransferRemote(AddressedTuple addressedTuple, Queue<AddressedTuple> pendingEmits, ITupleSerializer serializer) {
        if (pendingEmits != null && !pendingEmits.isEmpty()) {
            pendingEmits.add(addressedTuple);
            return false;
        }

        if (!remoteBackPressureStatus[addressedTuple.dest].get()) {
            TaskMessage tm = new TaskMessage(addressedTuple.getDest(), serializer.serialize(addressedTuple.getTuple()));
            if (transferQueue.tryPublish(tm)) {
                return true;
            }
        } else {
            LOG.debug("Noticed Back Pressure in remote task {}", addressedTuple.dest);
        }
        if (pendingEmits != null) {
            pendingEmits.add(addressedTuple);
        }
        return false;
    }

    public void flushRemotes() throws InterruptedException {
        transferQueue.flush();
    }

    public boolean tryFlushRemotes() {
        return transferQueue.tryFlush();
    }


    public void haltTransferThd() {
        transferQueue.close();
    }

}