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

import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;

public class BatchSpoutExecutor implements ITridentSpout<Object> {
    IBatchSpout spout;

    public BatchSpoutExecutor(IBatchSpout spout) {
        this.spout = spout;
    }

    @Override
    public BatchCoordinator<Object> getCoordinator(String txStateId, Map<String, Object> conf, TopologyContext context) {
        return new EmptyCoordinator();
    }

    @Override
    public Emitter<Object> getEmitter(String txStateId, Map<String, Object> conf, TopologyContext context) {
        spout.open(conf, context);
        return new BatchSpoutEmitter();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return spout.getComponentConfiguration();
    }

    @Override
    public Fields getOutputFields() {
        return spout.getOutputFields();
    }

    public static class EmptyCoordinator implements BatchCoordinator<Object> {
        @Override
        public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public void success(long txid) {
        }

        @Override
        public boolean isReady(long txid) {
            return true;
        }
    }

    public class BatchSpoutEmitter implements Emitter<Object> {

        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
            spout.emitBatch(tx.getTransactionId(), collector);
        }

        @Override
        public void success(TransactionAttempt tx) {
            spout.ack(tx.getTransactionId());
        }

        @Override
        public void close() {
            spout.close();
        }
    }

}
