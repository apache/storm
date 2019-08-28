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

package org.apache.storm.trident.topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.state.TransactionalState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.WindowedTimeThrottler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterBatchCoordinator extends BaseRichSpout {
    public static final Logger LOG = LoggerFactory.getLogger(MasterBatchCoordinator.class);

    public static final long INIT_TXID = 1L;


    public static final String BATCH_STREAM_ID = "$batch";
    public static final String COMMIT_STREAM_ID = "$commit";
    public static final String SUCCESS_STREAM_ID = "$success";

    private static final String CURRENT_TX = "currtx";
    private static final String CURRENT_ATTEMPTS = "currattempts";
    TreeMap<Long, TransactionStatus> activeTx = new TreeMap<Long, TransactionStatus>();
    TreeMap<Long, Integer> attemptIds;
    Long currTransaction;
    int maxTransactionActive;
    List<ITridentSpout.BatchCoordinator> coordinators = new ArrayList();
    List<String> managedSpoutIds;
    List<ITridentSpout> spouts;
    WindowedTimeThrottler throttler;
    boolean active = true;
    private List<TransactionalState> states = new ArrayList();
    private SpoutOutputCollector collector;

    public MasterBatchCoordinator(List<String> spoutIds, List<ITridentSpout> spouts) {
        if (spoutIds.isEmpty()) {
            throw new IllegalArgumentException("Must manage at least one spout");
        }
        managedSpoutIds = spoutIds;
        this.spouts = spouts;
        LOG.debug("Created {}", this);
    }

    public List<String> getManagedSpoutIds() {
        return managedSpoutIds;
    }

    @Override
    public void activate() {
        active = true;
    }

    @Override
    public void deactivate() {
        active = false;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        throttler = new WindowedTimeThrottler((Number) conf.get(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS), 1);
        for (String spoutId : managedSpoutIds) {
            states.add(TransactionalState.newCoordinatorState(conf, spoutId));
        }
        currTransaction = getStoredCurrTransaction();

        this.collector = collector;
        Number active = (Number) conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
        if (active == null) {
            maxTransactionActive = 1;
        } else {
            maxTransactionActive = active.intValue();
        }
        attemptIds = getStoredCurrAttempts(currTransaction, maxTransactionActive);


        for (int i = 0; i < spouts.size(); i++) {
            String txId = managedSpoutIds.get(i);
            coordinators.add(spouts.get(i).getCoordinator(txId, conf, context));
        }
        LOG.debug("Opened {}", this);
    }

    @Override
    public void close() {
        for (TransactionalState state : states) {
            state.close();
        }
        LOG.debug("Closed {}", this);
    }

    @Override
    public void nextTuple() {
        sync();
    }

    @Override
    public void ack(Object msgId) {
        TransactionAttempt tx = (TransactionAttempt) msgId;
        TransactionStatus status = activeTx.get(tx.getTransactionId());
        LOG.debug("Ack. [tx_attempt = {}], [tx_status = {}], [{}]", tx, status, this);
        if (status != null && tx.equals(status.attempt)) {
            if (status.status == AttemptStatus.PROCESSING) {
                status.status = AttemptStatus.PROCESSED;
                LOG.debug("Changed status. [tx_attempt = {}] [tx_status = {}]", tx, status);
            } else if (status.status == AttemptStatus.COMMITTING) {
                activeTx.remove(tx.getTransactionId());
                attemptIds.remove(tx.getTransactionId());
                collector.emit(SUCCESS_STREAM_ID, new Values(tx));
                currTransaction = nextTransactionId(tx.getTransactionId());
                for (TransactionalState state : states) {
                    state.setData(CURRENT_TX, currTransaction);
                }
                LOG.debug("Emitted on [stream = {}], [tx_attempt = {}], [tx_status = {}], [{}]", SUCCESS_STREAM_ID, tx, status, this);
            }
            sync();
        }
    }

    @Override
    public void fail(Object msgId) {
        TransactionAttempt tx = (TransactionAttempt) msgId;
        TransactionStatus stored = activeTx.remove(tx.getTransactionId());
        LOG.debug("Fail. [tx_attempt = {}], [tx_status = {}], [{}]", tx, stored, this);
        if (stored != null && tx.equals(stored.attempt)) {
            activeTx.tailMap(tx.getTransactionId()).clear();
            sync();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // in partitioned example, in case an emitter task receives a later transaction than it's emitted so far,
        // when it sees the earlier txid it should know to emit nothing
        declarer.declareStream(BATCH_STREAM_ID, new Fields("tx"));
        declarer.declareStream(COMMIT_STREAM_ID, new Fields("tx"));
        declarer.declareStream(SUCCESS_STREAM_ID, new Fields("tx"));
    }

    private void sync() {
        // note that sometimes the tuples active may be less than max_spout_pending, e.g.
        // max_spout_pending = 3
        // tx 1, 2, 3 active, tx 2 is acked. there won't be a commit for tx 2 (because tx 1 isn't committed yet),
        // and there won't be a batch for tx 4 because there's max_spout_pending tx active
        TransactionStatus maybeCommit = activeTx.get(currTransaction);
        if (maybeCommit != null && maybeCommit.status == AttemptStatus.PROCESSED) {
            maybeCommit.status = AttemptStatus.COMMITTING;
            collector.emit(COMMIT_STREAM_ID, new Values(maybeCommit.attempt), maybeCommit.attempt);
            LOG.debug("Emitted on [stream = {}], [tx_status = {}], [{}]", COMMIT_STREAM_ID, maybeCommit, this);
        }

        if (active) {
            if (activeTx.size() < maxTransactionActive) {
                Long curr = currTransaction;
                for (int i = 0; i < maxTransactionActive; i++) {
                    if (!activeTx.containsKey(curr) && isReady(curr)) {
                        // by using a monotonically increasing attempt id, downstream tasks
                        // can be memory efficient by clearing out state for old attempts
                        // as soon as they see a higher attempt id for a transaction
                        Integer attemptId = attemptIds.get(curr);
                        if (attemptId == null) {
                            attemptId = 0;
                        } else {
                            attemptId++;
                        }
                        attemptIds.put(curr, attemptId);
                        for (TransactionalState state : states) {
                            state.setData(CURRENT_ATTEMPTS, attemptIds);
                        }

                        TransactionAttempt attempt = new TransactionAttempt(curr, attemptId);
                        final TransactionStatus newTransactionStatus = new TransactionStatus(attempt);
                        activeTx.put(curr, newTransactionStatus);
                        collector.emit(BATCH_STREAM_ID, new Values(attempt), attempt);
                        LOG.debug("Emitted on [stream = {}], [tx_attempt = {}], [tx_status = {}], [{}]", BATCH_STREAM_ID, attempt,
                                  newTransactionStatus, this);
                        throttler.markEvent();
                    }
                    curr = nextTransactionId(curr);
                }
            }
        }
    }

    private boolean isReady(long txid) {
        if (throttler.isThrottled()) {
            return false;
        }
        //TODO: make this strategy configurable?... right now it goes if anyone is ready
        for (ITridentSpout.BatchCoordinator coord : coordinators) {
            if (coord.isReady(txid)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        ret.registerSerialization(TransactionAttempt.class);
        return ret;
    }

    private Long nextTransactionId(Long id) {
        return id + 1;
    }

    private Long getStoredCurrTransaction() {
        Long ret = INIT_TXID;
        for (TransactionalState state : states) {
            Long curr = (Long) state.getData(CURRENT_TX);
            if (curr != null && curr.compareTo(ret) > 0) {
                ret = curr;
            }
        }
        return ret;
    }

    private TreeMap<Long, Integer> getStoredCurrAttempts(long currTransaction, int maxBatches) {
        TreeMap<Long, Integer> ret = new TreeMap<Long, Integer>();
        for (TransactionalState state : states) {
            Map<Object, Number> attempts = (Map) state.getData(CURRENT_ATTEMPTS);
            if (attempts == null) {
                attempts = new HashMap();
            }
            for (Entry<Object, Number> e : attempts.entrySet()) {
                // this is because json doesn't allow numbers as keys...
                // TODO: replace json with a better form of encoding
                Number txidObj;
                if (e.getKey() instanceof String) {
                    txidObj = Long.parseLong((String) e.getKey());
                } else {
                    txidObj = (Number) e.getKey();
                }
                long txid = ((Number) txidObj).longValue();
                int attemptId = ((Number) e.getValue()).intValue();
                Integer curr = ret.get(txid);
                if (curr == null || attemptId > curr) {
                    ret.put(txid, attemptId);
                }
            }
        }
        ret.headMap(currTransaction).clear();
        ret.tailMap(currTransaction + maxBatches - 1).clear();
        return ret;
    }

    @Override
    public String toString() {
        return "MasterBatchCoordinator{"
                + "states=" + states
                + ", activeTx=" + activeTx
                + ", attemptIds=" + attemptIds
                + ", collector=" + collector
                + ", currTransaction=" + currTransaction
                + ", maxTransactionActive=" + maxTransactionActive
                + ", coordinators=" + coordinators
                + ", managedSpoutIds=" + managedSpoutIds
                + ", spouts=" + spouts
                + ", throttler=" + throttler
                + ", active=" + active
                + "}";
    }

    private enum AttemptStatus {
        PROCESSING,
        PROCESSED,
        COMMITTING
    }

    private static class TransactionStatus {
        TransactionAttempt attempt;
        AttemptStatus status;

        TransactionStatus(TransactionAttempt attempt) {
            this.attempt = attempt;
            this.status = AttemptStatus.PROCESSING;
        }

        @Override
        public String toString() {
            return attempt.toString() + " <" + status.toString() + ">";
        }
    }
}
