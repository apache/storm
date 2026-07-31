/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.iceberg.examples;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Emits a bounded sequence of generated tuples and then stops, replaying any tuple that fails.
 *
 * <p>Each tuple's content is derived purely from its index, and the index is used as its message
 * id, so a failed tuple is re-emitted with exactly the same content. That makes the source
 * replayable, which is what {@code IcebergBolt}'s at-least-once delivery needs: a failed batch is
 * written again rather than lost.
 *
 * <p>Replay is what at-least-once means in practice here — a replayed tuple is appended a second
 * time and both copies stay visible in the table, since the sink writes no equality deletes.
 */
public class BoundedTupleSpout extends BaseRichSpout {

    @Serial
    private static final long serialVersionUID = 1L;

    private final long totalTuples;
    private final Fields outputFields;
    private final ValuesFactory valuesFactory;

    private transient SpoutOutputCollector collector;
    private transient long nextIndex;
    private transient int taskCount;
    private transient int taskIndex;

    public BoundedTupleSpout(long totalTuples, Fields outputFields, ValuesFactory valuesFactory) {
        this.totalTuples = totalTuples;
        this.outputFields = outputFields;
        this.valuesFactory = valuesFactory;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        // Each task walks its own stride of the sequence, so the spout can be parallelised without
        // any two tasks emitting the same row.
        this.taskCount = context.getComponentTasks(context.getThisComponentId()).size();
        this.taskIndex = context.getThisTaskIndex();
        this.nextIndex = taskIndex;
    }

    @Override
    public void nextTuple() {
        if (nextIndex >= totalTuples) {
            return;
        }
        long index = nextIndex;
        nextIndex += taskCount;
        collector.emit(valuesFactory.create(index), index);
    }

    @Override
    public void fail(Object msgId) {
        long index = (Long) msgId;
        collector.emit(valuesFactory.create(index), index);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(outputFields);
    }

    /** Builds the tuple at a given position in the sequence. */
    public interface ValuesFactory extends Serializable {
        Values create(long index);
    }
}
