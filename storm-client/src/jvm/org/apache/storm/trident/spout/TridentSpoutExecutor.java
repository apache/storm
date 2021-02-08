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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.BatchInfo;
import org.apache.storm.trident.topology.ITridentBatchBolt;
import org.apache.storm.trident.topology.MasterBatchCoordinator;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.tuple.ConsList;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TridentSpoutExecutor implements ITridentBatchBolt {
    public static final String ID_FIELD = "$tx";

    public static final Logger LOG = LoggerFactory.getLogger(TridentSpoutExecutor.class);

    AddIdCollector collector;
    ITridentSpout<Object> spout;
    ITridentSpout.Emitter<Object> emitter;
    String streamName;
    String txStateId;

    TreeMap<Long, TransactionAttempt> activeBatches = new TreeMap<>();

    public TridentSpoutExecutor(String txStateId, String streamName, ITridentSpout<Object> spout) {
        this.txStateId = txStateId;
        this.spout = spout;
        this.streamName = streamName;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, BatchOutputCollector collector) {
        emitter = spout.getEmitter(txStateId, conf, context);
        this.collector = new AddIdCollector(streamName, collector);
    }

    @Override
    public void execute(BatchInfo info, Tuple input) {
        // there won't be a BatchInfo for the success stream
        TransactionAttempt attempt = (TransactionAttempt) input.getValue(0);
        if (input.getSourceStreamId().equals(MasterBatchCoordinator.COMMIT_STREAM_ID)) {
            if (attempt.equals(activeBatches.get(attempt.getTransactionId()))) {
                ((ICommitterTridentSpout.Emitter) emitter).commit(attempt);
                activeBatches.remove(attempt.getTransactionId());
            } else {
                throw new FailedException("Received commit for different transaction attempt");
            }
        } else if (input.getSourceStreamId().equals(MasterBatchCoordinator.SUCCESS_STREAM_ID)) {
            // valid to delete before what's been committed since 
            // those batches will never be accessed again
            activeBatches.headMap(attempt.getTransactionId()).clear();
            emitter.success(attempt);
        } else {
            collector.setBatch(info.batchId);
            emitter.emitBatch(attempt, input.getValue(1), collector);
            activeBatches.put(attempt.getTransactionId(), attempt);
        }
    }

    @Override
    public void cleanup() {
        emitter.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fields = new ArrayList<>(spout.getOutputFields().toList());
        fields.add(0, ID_FIELD);
        declarer.declareStream(streamName, new Fields(fields));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return spout.getComponentConfiguration();
    }

    @Override
    public void finishBatch(BatchInfo batchInfo) {
    }

    @Override
    public Object initBatchState(String batchGroup, Object batchId) {
        return null;
    }

    private static class AddIdCollector implements TridentCollector {
        BatchOutputCollector delegate;
        Object id;
        String stream;

        AddIdCollector(String stream, BatchOutputCollector c) {
            delegate = c;
            this.stream = stream;
        }


        public void setBatch(Object id) {
            this.id = id;
        }

        @Override
        public void emit(List<Object> values) {
            delegate.emit(stream, new ConsList(id, values));
        }

        @Override
        public void flush() {
            delegate.flush();
        }

        @Override
        public void reportError(Throwable t) {
            delegate.reportError(t);
        }
    }
}
