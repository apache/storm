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

package org.apache.storm.trident.spout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.topology.state.RotatingTransactionalState;
import org.apache.storm.trident.topology.state.TransactionalState;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartitionedTridentSpoutExecutor implements ITridentSpout<Object> {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionedTridentSpoutExecutor.class);

    IPartitionedTridentSpout<Object, ISpoutPartition, Object> spout;

    public PartitionedTridentSpoutExecutor(IPartitionedTridentSpout<Object, ISpoutPartition, Object> spout) {
        this.spout = spout;
    }

    public IPartitionedTridentSpout<Object, ISpoutPartition, Object> getPartitionedSpout() {
        return spout;
    }

    @Override
    public ITridentSpout.BatchCoordinator<Object> getCoordinator(String txStateId, Map<String, Object> conf, TopologyContext context) {
        return new Coordinator(conf, context);
    }

    @Override
    public ITridentSpout.Emitter<Object> getEmitter(String txStateId, Map<String, Object> conf, TopologyContext context) {
        return new Emitter(txStateId, conf, context);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return spout.getComponentConfiguration();
    }

    @Override
    public Fields getOutputFields() {
        return spout.getOutputFields();
    }

    static class EmitterPartitionState {
        public RotatingTransactionalState rotatingState;
        public ISpoutPartition partition;

        EmitterPartitionState(RotatingTransactionalState s, ISpoutPartition p) {
            rotatingState = s;
            partition = p;
        }
    }

    class Coordinator implements ITridentSpout.BatchCoordinator<Object> {
        private IPartitionedTridentSpout.Coordinator<Object> coordinator;

        Coordinator(Map<String, Object> conf, TopologyContext context) {
            coordinator = spout.getCoordinator(conf, context);
        }

        @Override
        public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
            LOG.debug("Initialize Transaction. txid = {}, prevMetadata = {}, currMetadata = {}", txid, prevMetadata, currMetadata);

            if (currMetadata != null) {
                return currMetadata;
            } else {
                return coordinator.getPartitionsForBatch();
            }
        }


        @Override
        public void close() {
            LOG.debug("Closing");
            coordinator.close();
            LOG.debug("Closed");
        }

        @Override
        public void success(long txid) {
            LOG.debug("Success transaction id " + txid);
        }

        @Override
        public boolean isReady(long txid) {
            boolean ready = coordinator.isReady(txid);
            LOG.debug("isReady = {} ", ready);
            return ready;
        }
    }

    class Emitter implements ITridentSpout.Emitter<Object> {
        Object savedCoordinatorMeta = null;
        private IPartitionedTridentSpout.Emitter<Object, ISpoutPartition, Object> emitter;
        private TransactionalState state;
        private Map<String, EmitterPartitionState> partitionStates = new HashMap<>();
        private int index;
        private int numTasks;

        Emitter(String txStateId, Map<String, Object> conf, TopologyContext context) {
            emitter = spout.getEmitter(conf, context);
            state = TransactionalState.newUserState(conf, txStateId);
            index = context.getThisTaskIndex();
            numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        }

        @Override
        public void emitBatch(final TransactionAttempt tx, final Object coordinatorMeta, final TridentCollector collector) {
            LOG.debug("Emitting Batch. [transaction = {}], [coordinatorMeta = {}], [collector = {}]", tx, coordinatorMeta, collector);

            if (savedCoordinatorMeta == null || !savedCoordinatorMeta.equals(coordinatorMeta)) {
                partitionStates.clear();
                List<ISpoutPartition> taskPartitions = emitter.getPartitionsForTask(index, numTasks,
                        emitter.getOrderedPartitions(coordinatorMeta));
                for (ISpoutPartition partition : taskPartitions) {
                    partitionStates.put(partition.getId(),
                            new EmitterPartitionState(new RotatingTransactionalState(state, partition.getId()), partition));
                }

                emitter.refreshPartitions(taskPartitions);
                savedCoordinatorMeta = coordinatorMeta;
            }
            for (EmitterPartitionState s : partitionStates.values()) {
                RotatingTransactionalState state = s.rotatingState;
                final ISpoutPartition partition = s.partition;
                Object meta = state.getStateOrCreate(tx.getTransactionId(),
                        new RotatingTransactionalState.StateInitializer() {
                            @Override
                            public Object init(long txid, Object lastState) {
                                return emitter.emitPartitionBatchNew(tx, collector, partition, lastState);
                            }
                        });
                // it's null if one of:
                //   a) a later transaction batch was emitted before this, so we should skip this batch
                //   b) if didn't exist and was created (in which case the StateInitializer was invoked and
                //      it was emitted
                if (meta != null) {
                    emitter.emitPartitionBatch(tx, collector, partition, meta);
                }
            }
            LOG.debug("Emitted Batch. [tx = {}], [coordinatorMeta = {}], [collector = {}]", tx, coordinatorMeta, collector);
        }

        @Override
        public void success(TransactionAttempt tx) {
            LOG.debug("Success transaction " + tx);
            for (EmitterPartitionState state : partitionStates.values()) {
                state.rotatingState.cleanupBefore(tx.getTransactionId());
            }
        }

        @Override
        public void close() {
            LOG.debug("Closing");
            state.close();
            emitter.close();
            LOG.debug("Closed");
        }
    }
}
