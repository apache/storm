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

import com.google.common.annotations.VisibleForTesting;
import com.lmax.disruptor.EventHandler;
import org.apache.storm.Config;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.MutableObject;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;

public class ExecutorTransfer implements EventHandler, Callable {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorTransfer.class);

    private final WorkerState workerData;
    private final DisruptorQueue batchTransferQueue;
    private final Map stormConf;
    private final KryoTupleSerializer serializer;
    private final MutableObject cachedEmit;
    private final boolean isDebug;

    public ExecutorTransfer(WorkerState workerData, DisruptorQueue batchTransferQueue, Map stormConf) {
        this.workerData = workerData;
        this.batchTransferQueue = batchTransferQueue;
        this.stormConf = stormConf;
        this.serializer = new KryoTupleSerializer(stormConf, workerData.getWorkerTopologyContext());
        this.cachedEmit = new MutableObject(new ArrayList<>());
        this.isDebug = Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
    }

    public void transfer(int task, Tuple tuple) {
        AddressedTuple val = new AddressedTuple(task, tuple);
        if (isDebug) {
            LOG.info("TRANSFERRING tuple {}", val);
        }
        batchTransferQueue.publish(val);
    }

    @VisibleForTesting
    public DisruptorQueue getBatchTransferQueue() {
        return this.batchTransferQueue;
    }

    @Override
    public Object call() throws Exception {
        batchTransferQueue.consumeBatchWhenAvailable(this);
        return 0L;
    }

    public String getName() {
        return batchTransferQueue.getName();
    }

    @Override
    public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
        ArrayList cachedEvents = (ArrayList) cachedEmit.getObject();
        cachedEvents.add(event);
        if (endOfBatch) {
            workerData.transfer(serializer, cachedEvents);
            cachedEmit.setObject(new ArrayList<>());
        }
    }
}
