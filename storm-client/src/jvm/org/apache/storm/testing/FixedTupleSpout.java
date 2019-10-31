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

package org.apache.storm.testing;

import static org.apache.storm.utils.Utils.get;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

public class FixedTupleSpout implements IRichSpout, CompletableSpout {
    private static final Map<String, Integer> acked = new HashMap<String, Integer>();
    private static final Map<String, Integer> failed = new HashMap<String, Integer>();
    private List<FixedTuple> tuples;
    private SpoutOutputCollector collector;
    private TopologyContext context;
    private List<FixedTuple> serveTuples;
    private Map<String, FixedTuple> pending;
    private String id;
    private Fields fields;

    public FixedTupleSpout(List tuples) {
        this(tuples, (Fields) null);
    }

    public FixedTupleSpout(List tuples, Fields fields) {
        id = UUID.randomUUID().toString();
        synchronized (acked) {
            acked.put(id, 0);
        }
        synchronized (failed) {
            failed.put(id, 0);
        }
        this.tuples = new ArrayList<FixedTuple>();
        for (Object o : tuples) {
            FixedTuple ft;
            if (o instanceof FixedTuple) {
                ft = (FixedTuple) o;
            } else {
                ft = new FixedTuple((List) o);
            }
            this.tuples.add(ft);
        }
        this.fields = fields;
    }

    public static int getNumAcked(String stormId) {
        synchronized (acked) {
            return get(acked, stormId, 0);
        }
    }

    public static int getNumFailed(String stormId) {
        synchronized (failed) {
            return get(failed, stormId, 0);
        }
    }

    public static void clear(String stormId) {
        acked.remove(stormId);
        failed.remove(stormId);
    }

    public List<FixedTuple> getSourceTuples() {
        return tuples;
    }

    public int getCompleted() {
        int ackedAmt;
        int failedAmt;

        synchronized (acked) {
            ackedAmt = acked.get(id);
        }
        synchronized (failed) {
            failedAmt = failed.get(id);
        }
        return ackedAmt + failedAmt;
    }

    public void cleanup() {
        synchronized (acked) {
            acked.remove(id);
        }
        synchronized (failed) {
            failed.remove(id);
        }
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        List<Integer> tasks = context.getComponentTasks(context.getThisComponentId());
        int startIndex;
        for (startIndex = 0; startIndex < tasks.size(); startIndex++) {
            if (tasks.get(startIndex) == context.getThisTaskId()) {
                break;
            }
        }
        this.collector = collector;
        pending = new HashMap<String, FixedTuple>();
        serveTuples = new ArrayList<FixedTuple>();
        for (int i = startIndex; i < tuples.size(); i += tasks.size()) {
            serveTuples.add(tuples.get(i));
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void nextTuple() {
        if (serveTuples.size() > 0) {
            FixedTuple ft = serveTuples.remove(0);
            String id = UUID.randomUUID().toString();
            pending.put(id, ft);
            collector.emit(ft.stream, ft.values, id);
        }
    }

    @Override
    public void ack(Object msgId) {
        synchronized (acked) {
            int curr = get(acked, id, 0);
            acked.put(id, curr + 1);
        }
    }

    @Override
    public void fail(Object msgId) {
        synchronized (failed) {
            int curr = get(failed, id, 0);
            failed.put(id, curr + 1);
        }
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (fields != null) {
            declarer.declare(fields);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public boolean isExhausted() {
        return getSourceTuples().size() == getCompleted();
    }
}
