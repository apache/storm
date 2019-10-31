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

package org.apache.storm.trident.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.topology.TridentTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.RegisteredGlobalState;
import org.apache.storm.utils.Utils;

public class FeederBatchSpout implements ITridentSpout<Map<Integer, List<List<Object>>>>, IFeeder {

    String id;
    String semaphoreId;
    Fields outFields;
    boolean waitToEmit = true;


    public FeederBatchSpout(List<String> fields) {
        outFields = new Fields(fields);
        id = RegisteredGlobalState.registerState(new CopyOnWriteArrayList());
        semaphoreId = RegisteredGlobalState.registerState(new CopyOnWriteArrayList());
    }

    public void setWaitToEmit(boolean trueIfWait) {
        waitToEmit = trueIfWait;
    }

    @Override
    public void feed(Object tuples) {
        Semaphore sem = new Semaphore(0);
        ((List) RegisteredGlobalState.getState(semaphoreId)).add(sem);
        ((List) RegisteredGlobalState.getState(id)).add(tuples);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return outFields;
    }

    @Override
    public BatchCoordinator<Map<Integer, List<List<Object>>>> getCoordinator(String txStateId, Map<String, Object> conf,
                                                                             TopologyContext context) {
        int numTasks = context.getComponentTasks(
            TridentTopologyBuilder.spoutIdFromCoordinatorId(
                context.getThisComponentId()))
                              .size();
        return new FeederCoordinator(numTasks);
    }

    @Override
    public Emitter<Map<Integer, List<List<Object>>>> getEmitter(String txStateId, Map<String, Object> conf, TopologyContext context) {
        return new FeederEmitter(context.getThisTaskIndex());
    }

    private static class FeederEmitter implements ITridentSpout.Emitter<Map<Integer, List<List<Object>>>> {

        int index;

        FeederEmitter(int index) {
            this.index = index;
        }

        @Override
        public void emitBatch(TransactionAttempt tx, Map<Integer, List<List<Object>>> coordinatorMeta, TridentCollector collector) {
            List<List<Object>> tuples = coordinatorMeta.get(index);
            if (tuples != null) {
                for (List<Object> t : tuples) {
                    collector.emit(t);
                }
            }
        }

        @Override
        public void success(TransactionAttempt tx) {
        }

        @Override
        public void close() {
        }
    }

    public class FeederCoordinator implements ITridentSpout.BatchCoordinator<Map<Integer, List<List<Object>>>> {

        int numPartitions;
        int emittedIndex = 0;
        Map<Long, Integer> txIndices = new HashMap();
        int masterEmitted = 0;

        public FeederCoordinator(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        @Override
        public Map<Integer, List<List<Object>>> initializeTransaction(long txid, Map<Integer, List<List<Object>>> prevMetadata,
                                                                      Map<Integer, List<List<Object>>> currMetadata) {
            if (currMetadata != null) {
                return currMetadata;
            }
            List allBatches = (List) RegisteredGlobalState.getState(id);
            if (allBatches.size() > emittedIndex) {
                Object batchInfo = allBatches.get(emittedIndex);
                txIndices.put(txid, emittedIndex);
                emittedIndex += 1;
                if (batchInfo instanceof Map) {
                    return (Map) batchInfo;
                } else {
                    List batchList = (List) batchInfo;
                    Map<Integer, List<List<Object>>> partitions = new HashMap();
                    for (int i = 0; i < numPartitions; i++) {
                        partitions.put(i, new ArrayList());
                    }
                    for (int i = 0; i < batchList.size(); i++) {
                        int partition = i % numPartitions;
                        partitions.get(partition).add((List) batchList.get(i));
                    }
                    return partitions;
                }
            } else {
                return new HashMap();
            }
        }

        @Override
        public void close() {
        }

        @Override
        public void success(long txid) {
            Integer index = txIndices.get(txid);
            if (index != null) {
                Semaphore sem = (Semaphore) ((List) RegisteredGlobalState.getState(semaphoreId)).get(index);
                sem.release();
            }
        }

        @Override
        public boolean isReady(long txid) {
            if (!waitToEmit) {
                return true;
            }
            List allBatches = (List) RegisteredGlobalState.getState(id);
            if (allBatches.size() > masterEmitted) {
                masterEmitted++;
                return true;
            } else {
                Utils.sleep(2);
                return false;
            }
        }
    }


}
