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
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.topology.MasterBatchCoordinator;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.topology.state.RotatingTransactionalState;
import org.apache.storm.trident.topology.state.TransactionalState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TridentSpoutCoordinator implements IBasicBolt {
    public static final Logger LOG = LoggerFactory.getLogger(TridentSpoutCoordinator.class);
    private static final String META_DIR = "meta";

    ITridentSpout<Object> spout;
    ITridentSpout.BatchCoordinator<Object> coord;
    RotatingTransactionalState state;
    TransactionalState underlyingState;
    String id;


    public TridentSpoutCoordinator(String id, ITridentSpout<Object> spout) {
        this.spout = spout;
        this.id = id;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context) {
        coord = spout.getCoordinator(id, conf, context);
        underlyingState = TransactionalState.newCoordinatorState(conf, id);
        state = new RotatingTransactionalState(underlyingState, META_DIR);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        TransactionAttempt attempt = (TransactionAttempt) tuple.getValue(0);

        if (tuple.getSourceStreamId().equals(MasterBatchCoordinator.SUCCESS_STREAM_ID)) {
            state.cleanupBefore(attempt.getTransactionId());
            coord.success(attempt.getTransactionId());
        } else {
            long txid = attempt.getTransactionId();
            Object prevMeta = state.getPreviousState(txid);
            Object meta = coord.initializeTransaction(txid, prevMeta, state.getState(txid));
            state.overrideState(txid, meta);
            collector.emit(MasterBatchCoordinator.BATCH_STREAM_ID, new Values(attempt, meta));
        }

    }

    @Override
    public void cleanup() {
        coord.close();
        underlyingState.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(MasterBatchCoordinator.BATCH_STREAM_ID, new Fields("tx", "metadata"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }
}
