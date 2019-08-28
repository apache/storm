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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.topology.state.RotatingTransactionalState;
import org.apache.storm.trident.topology.state.TransactionalState;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OpaquePartitionedTridentSpoutExecutor implements ICommitterTridentSpout<Object> {
    protected static final Logger LOG = LoggerFactory.getLogger(OpaquePartitionedTridentSpoutExecutor.class);

    IOpaquePartitionedTridentSpout<Object, ISpoutPartition, Object> spout;

    public OpaquePartitionedTridentSpoutExecutor(IOpaquePartitionedTridentSpout<Object, ISpoutPartition, Object> spout) {
        this.spout = spout;
    }

    @Override
    public ITridentSpout.BatchCoordinator<Object> getCoordinator(String txStateId, Map<String, Object> conf, TopologyContext context) {
        return new Coordinator(conf, context);
    }

    @Override
    public ICommitterTridentSpout.Emitter getEmitter(String txStateId, Map<String, Object> conf, TopologyContext context) {
        return new Emitter(txStateId, conf, context);
    }

    @Override
    public Fields getOutputFields() {
        return spout.getOutputFields();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return spout.getComponentConfiguration();
    }

    static class EmitterPartitionState {
        public RotatingTransactionalState rotatingState;
        public ISpoutPartition partition;

        EmitterPartitionState(RotatingTransactionalState s, ISpoutPartition p) {
            rotatingState = s;
            partition = p;
        }
    }

    public class Coordinator implements ITridentSpout.BatchCoordinator<Object> {
        IOpaquePartitionedTridentSpout.Coordinator coordinator;

        public Coordinator(Map<String, Object> conf, TopologyContext context) {
            coordinator = spout.getCoordinator(conf, context);
        }

        @Override
        public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
            LOG.debug("Initialize Transaction. [txid = {}], [prevMetadata = {}], [currMetadata = {}]", txid, prevMetadata, currMetadata);
            return coordinator.getPartitionsForBatch();
        }


        @Override
        public void close() {
            LOG.debug("Closing");
            coordinator.close();
            LOG.debug("Closed");
        }

        @Override
        public void success(long txid) {
            LOG.debug("Success [txid = {}]", txid);
        }

        @Override
        public boolean isReady(long txid) {
            boolean ready = coordinator.isReady(txid);
            LOG.debug("[isReady = {}], [txid = {}]", ready, txid);
            return ready;
        }
    }

    public class Emitter implements ICommitterTridentSpout.Emitter {
        IOpaquePartitionedTridentSpout.Emitter<Object, ISpoutPartition, Object> emitter;
        TransactionalState state;
        TreeMap<Long, Map<String, Object>> cachedMetas = new TreeMap<>();
        Map<String, EmitterPartitionState> partitionStates = new HashMap<>();
        int index;
        int numTasks;
        Object savedCoordinatorMeta = null;
        boolean changedMeta = false;

        public Emitter(String txStateId, Map<String, Object> conf, TopologyContext context) {
            emitter = spout.getEmitter(conf, context);
            index = context.getThisTaskIndex();
            numTasks = context.getComponentTasks(context.getThisComponentId()).size();
            state = TransactionalState.newUserState(conf, txStateId);
            LOG.debug("Created {}", this);
        }

        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
            LOG.debug("Emitting Batch. [transaction = {}], [coordinatorMeta = {}], [collector = {}], [{}]",
                      tx, coordinatorMeta, collector, this);

            if (savedCoordinatorMeta == null || !savedCoordinatorMeta.equals(coordinatorMeta)) {
                partitionStates.clear();
                final List<ISpoutPartition> sortedPartitions = emitter.getOrderedPartitions(coordinatorMeta);
                final List<ISpoutPartition> taskPartitions = emitter.getPartitionsForTask(index, numTasks, sortedPartitions);
                for (ISpoutPartition partition : taskPartitions) {
                    partitionStates.put(partition.getId(),
                                         new EmitterPartitionState(new RotatingTransactionalState(state, partition.getId()), partition));
                }
                emitter.refreshPartitions(taskPartitions);

                savedCoordinatorMeta = coordinatorMeta;
                changedMeta = true;
            }
            Map<String, Object> metas = new HashMap<>();
            cachedMetas.put(tx.getTransactionId(), metas);

            Entry<Long, Map<String, Object>> entry = cachedMetas.lowerEntry(tx.getTransactionId());
            Map<String, Object> prevCached;
            if (entry != null) {
                prevCached = entry.getValue();
            } else {
                prevCached = new HashMap<>();
            }

            for (Entry<String, EmitterPartitionState> e : partitionStates.entrySet()) {
                String id = e.getKey();
                EmitterPartitionState s = e.getValue();
                s.rotatingState.removeState(tx.getTransactionId());
                Object lastMeta = prevCached.get(id);
                if (lastMeta == null) {
                    lastMeta = s.rotatingState.getLastState();
                }
                Object meta = emitter.emitPartitionBatch(tx, collector, s.partition, lastMeta);
                metas.put(id, meta);
            }
            LOG.debug("Emitted Batch. [transaction = {}], [coordinatorMeta = {}], [collector = {}], [{}]",
                      tx, coordinatorMeta, collector, this);
        }

        @Override
        public void success(TransactionAttempt tx) {
            for (EmitterPartitionState state : partitionStates.values()) {
                state.rotatingState.cleanupBefore(tx.getTransactionId());
            }
            LOG.debug("Success transaction {}. [{}]", tx, this);
        }

        @Override
        public void commit(TransactionAttempt attempt) {
            LOG.debug("Committing transaction {}. [{}]", attempt, this);
            // this code here handles a case where a previous commit failed, and the partitions
            // changed since the last commit. This clears out any state for the removed partitions
            // for this txid.
            // we make sure only a single task ever does this. we're also guaranteed that
            // it's impossible for there to be another writer to the directory for that partition
            // because only a single commit can be happening at once. this is because in order for
            // another attempt of the batch to commit, the batch phase must have succeeded in between.
            // hence, all tasks for the prior commit must have finished committing (whether successfully or not)
            if (changedMeta && index == 0) {
                Set<String> validIds = new HashSet<>();
                for (ISpoutPartition p : emitter.getOrderedPartitions(savedCoordinatorMeta)) {
                    validIds.add(p.getId());
                }
                for (String existingPartition : state.list("")) {
                    if (!validIds.contains(existingPartition)) {
                        RotatingTransactionalState s = new RotatingTransactionalState(state, existingPartition);
                        s.removeState(attempt.getTransactionId());
                    }
                }
                changedMeta = false;
            }

            Long txid = attempt.getTransactionId();
            Map<String, Object> metas = cachedMetas.remove(txid);
            for (Entry<String, Object> entry : metas.entrySet()) {
                partitionStates.get(entry.getKey()).rotatingState.overrideState(txid, entry.getValue());
            }
            LOG.debug("Exiting commit method for transaction {}. [{}]", attempt, this);
        }

        @Override
        public void close() {
            LOG.debug("Closing");
            emitter.close();
            LOG.debug("Closed");
        }

        @Override
        public String toString() {
            return "Emitter{"
                    + ", state=" + state
                    + ", cachedMetas=" + cachedMetas
                    + ", partitionStates=" + partitionStates
                    + ", index=" + index
                    + ", numTasks=" + numTasks
                    + ", savedCoordinatorMeta=" + savedCoordinatorMeta
                    + ", changedMeta=" + changedMeta
                    + '}';
        }
    }

}
