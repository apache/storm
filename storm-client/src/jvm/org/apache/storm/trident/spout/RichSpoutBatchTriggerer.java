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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.generated.Grouping;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.topology.TridentBoltExecutor;
import org.apache.storm.trident.tuple.ConsList;
import org.apache.storm.trident.util.TridentUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


public class RichSpoutBatchTriggerer implements IRichSpout {

    String stream;
    IRichSpout delegate;
    List<Integer> outputTasks;
    Random rand;
    String coordStream;
    Map<Long, Long> msgIdToBatchId = new HashMap<>();
    Map<Long, FinishCondition> finishConditions = new HashMap<>();

    public RichSpoutBatchTriggerer(IRichSpout delegate, String streamName, String batchGroup) {
        this.delegate = delegate;
        stream = streamName;
        coordStream = TridentBoltExecutor.coordStream(batchGroup);
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        delegate.open(conf, context, new SpoutOutputCollector(new StreamOverrideCollector(collector)));
        outputTasks = new ArrayList<>();
        for (String component : Utils.get(context.getThisTargets(),
                coordStream,
                                          new HashMap<String, Grouping>()).keySet()) {
            outputTasks.addAll(context.getComponentTasks(component));
        }
        rand = new Random(Utils.secureRandomLong());
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void activate() {
        delegate.activate();
    }

    @Override
    public void deactivate() {
        delegate.deactivate();
    }

    @Override
    public void nextTuple() {
        delegate.nextTuple();
    }

    @Override
    public void ack(Object msgId) {
        Long batchId = msgIdToBatchId.remove((Long) msgId);
        FinishCondition cond = finishConditions.get(batchId);
        if (cond != null) {
            cond.vals.remove((Long) msgId);
            if (cond.vals.isEmpty()) {
                finishConditions.remove(batchId);
                delegate.ack(cond.msgId);
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        Long batchId = msgIdToBatchId.remove((Long) msgId);
        FinishCondition cond = finishConditions.remove(batchId);
        if (cond != null) {
            delegate.fail(cond.msgId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields outFields = TridentUtils.getSingleOutputStreamFields(delegate);
        outFields = TridentUtils.fieldsConcat(new Fields("$id$"), outFields);
        declarer.declareStream(stream, outFields);
        // try to find a way to merge this code with what's already done in TridentBoltExecutor
        declarer.declareStream(coordStream, true, new Fields("id", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = delegate.getComponentConfiguration();
        if (conf == null) {
            conf = new HashMap<>();
        } else {
            conf = new HashMap<>(conf);
        }
        Config.registerSerialization(conf, RichSpoutBatchId.class, RichSpoutBatchIdSerializer.class);
        return conf;
    }

    static class FinishCondition {
        Set<Long> vals = new HashSet<>();
        Object msgId;
    }

    class StreamOverrideCollector implements ISpoutOutputCollector {

        SpoutOutputCollector collector;

        StreamOverrideCollector(SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public List<Integer> emit(String ignore, List<Object> values, Object msgId) {
            long batchIdVal = rand.nextLong();
            Object batchId = new RichSpoutBatchId(batchIdVal);
            FinishCondition finish = new FinishCondition();
            finish.msgId = msgId;
            List<Integer> tasks = collector.emit(stream, new ConsList(batchId, values));
            Set<Integer> outTasksSet = new HashSet<>(tasks);
            for (Integer t : outputTasks) {
                int count = 0;
                if (outTasksSet.contains(t)) {
                    count = 1;
                }
                long r = rand.nextLong();
                collector.emitDirect(t, coordStream, new Values(batchId, count), r);
                finish.vals.add(r);
                msgIdToBatchId.put(r, batchIdVal);
            }
            finishConditions.put(batchIdVal, finish);
            return tasks;
        }

        @Override
        public void emitDirect(int task, String ignore, List<Object> values, Object msgId) {
            throw new RuntimeException("Trident does not support direct emits from spouts");
        }

        @Override
        public void flush() {
            collector.flush();
        }

        @Override
        public void reportError(Throwable t) {
            collector.reportError(t);
        }

        @Override
        public long getPendingCount() {
            return collector.getPendingCount();
        }
    }
}
