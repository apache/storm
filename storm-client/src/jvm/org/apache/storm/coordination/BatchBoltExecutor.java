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

package org.apache.storm.coordination;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.coordination.CoordinatedBolt.FinishedCallback;
import org.apache.storm.coordination.CoordinatedBolt.TimeoutCallback;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchBoltExecutor implements IRichBolt, FinishedCallback, TimeoutCallback {
    public static final Logger LOG = LoggerFactory.getLogger(BatchBoltExecutor.class);

    private byte[] boltSer;
    private Map<Object, IBatchBolt> openTransactions;
    private Map conf;
    private TopologyContext context;
    private BatchOutputCollectorImpl collector;

    public BatchBoltExecutor(IBatchBolt bolt) {
        boltSer = Utils.javaSerialize(bolt);
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = new BatchOutputCollectorImpl(collector);
        openTransactions = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        Object id = input.getValue(0);
        IBatchBolt bolt = getBatchBolt(id);
        try {
            bolt.execute(input);
            collector.ack(input);
        } catch (FailedException e) {
            LOG.error("Failed to process tuple in batch", e);
            collector.fail(input);
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void finishedId(Object id) {
        IBatchBolt bolt = getBatchBolt(id);
        openTransactions.remove(id);
        bolt.finishBatch();
    }

    @Override
    public void timeoutId(Object attempt) {
        openTransactions.remove(attempt);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        newTransactionalBolt().declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return newTransactionalBolt().getComponentConfiguration();
    }

    private IBatchBolt getBatchBolt(Object id) {
        IBatchBolt bolt = openTransactions.get(id);
        if (bolt == null) {
            bolt = newTransactionalBolt();
            bolt.prepare(conf, context, collector, id);
            openTransactions.put(id, bolt);
        }
        return bolt;
    }

    private IBatchBolt newTransactionalBolt() {
        return Utils.javaDeserialize(boltSer, IBatchBolt.class);
    }
}
