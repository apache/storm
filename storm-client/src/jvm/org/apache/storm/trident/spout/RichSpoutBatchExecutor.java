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
import org.apache.storm.Config;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.util.TridentUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.RotatingMap;

public class RichSpoutBatchExecutor implements ITridentSpout<Object> {
    public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";

    IRichSpout spout;

    public RichSpoutBatchExecutor(IRichSpout spout) {
        this.spout = spout;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return spout.getComponentConfiguration();
    }

    @Override
    public Fields getOutputFields() {
        return TridentUtils.getSingleOutputStreamFields(spout);

    }

    @Override
    public BatchCoordinator<Object> getCoordinator(String txStateId, Map<String, Object> conf, TopologyContext context) {
        return new RichSpoutCoordinator();
    }

    @Override
    public Emitter<Object> getEmitter(String txStateId, Map<String, Object> conf, TopologyContext context) {
        return new RichSpoutEmitter(conf, context);
    }

    private static class RichSpoutCoordinator implements ITridentSpout.BatchCoordinator<Object> {
        @Override
        public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
            return null;
        }

        @Override
        public void success(long txid) {
        }

        @Override
        public boolean isReady(long txid) {
            return true;
        }

        @Override
        public void close() {
        }
    }

    static class CaptureCollector implements ISpoutOutputCollector {

        public List<Object> ids;
        public int numEmitted;
        public long pendingCount;
        TridentCollector collector;

        public void reset(TridentCollector c) {
            collector = c;
            ids = new ArrayList<>();
        }

        @Override
        public void reportError(Throwable t) {
            collector.reportError(t);
        }

        @Override
        public List<Integer> emit(String stream, List<Object> values, Object id) {
            if (id != null) {
                ids.add(id);
            }
            numEmitted++;
            collector.emit(values);
            return null;
        }

        @Override
        public void emitDirect(int task, String stream, List<Object> values, Object id) {
            throw new UnsupportedOperationException("Trident does not support direct streams");
        }

        @Override
        public void flush() {
            collector.flush();
        }

        @Override
        public long getPendingCount() {
            return pendingCount;
        }
    }

    class RichSpoutEmitter implements ITridentSpout.Emitter<Object> {
        int maxBatchSize;
        boolean prepared = false;
        CaptureCollector collector;
        RotatingMap<Long, List<Object>> idsMap;
        Map conf;
        TopologyContext context;
        long lastRotate = System.currentTimeMillis();
        long rotateTime;

        RichSpoutEmitter(Map<String, Object> conf, TopologyContext context) {
            this.conf = conf;
            this.context = context;
            Number batchSize = (Number) conf.get(MAX_BATCH_SIZE_CONF);
            if (batchSize == null) {
                batchSize = 1000;
            }
            maxBatchSize = batchSize.intValue();
            collector = new CaptureCollector();
            idsMap = new RotatingMap<>(3);
            rotateTime = 1000L * ((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
        }

        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
            long txid = tx.getTransactionId();

            long now = System.currentTimeMillis();
            if (now - lastRotate > rotateTime) {
                Map<Long, List<Object>> failed = idsMap.rotate();
                for (Long id : failed.keySet()) {
                    //TODO: this isn't right... it's not in the map anymore
                    fail(id);
                }
                lastRotate = now;
            }

            if (idsMap.containsKey(txid)) {
                fail(txid);
            }

            this.collector.reset(collector);
            if (!prepared) {
                spout.open(conf, context, new SpoutOutputCollector(this.collector));
                prepared = true;
            }
            for (int i = 0; i < maxBatchSize; i++) {
                spout.nextTuple();
                if (this.collector.numEmitted < i) {
                    break;
                }
            }
            idsMap.put(txid, this.collector.ids);
            this.collector.pendingCount = idsMap.size();

        }

        @Override
        public void success(TransactionAttempt tx) {
            ack(tx.getTransactionId());
        }

        private void ack(long batchId) {
            List<Object> ids = (List<Object>) idsMap.remove(batchId);
            if (ids != null) {
                for (Object id : ids) {
                    spout.ack(id);
                }
            }
        }

        private void fail(long batchId) {
            List<Object> ids = (List<Object>) idsMap.remove(batchId);
            if (ids != null) {
                for (Object id : ids) {
                    spout.fail(id);
                }
            }
        }

        @Override
        public void close() {
            spout.close();
        }

    }

}
