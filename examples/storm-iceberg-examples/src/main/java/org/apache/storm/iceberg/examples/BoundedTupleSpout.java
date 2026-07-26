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
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.topology.MasterBatchCoordinator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Emits a bounded, deterministic sequence of generated tuples and then stops.
 *
 * <p>Each tuple's content is derived purely from its index, and the index range of a batch is
 * derived purely from the batch id, so a replayed batch always carries the same tuples. That is
 * what makes this spout usable with the exactly-once semantics of {@code IcebergState}: a
 * cycling {@code FixedBatchSpout} would not be, and a fixed list cannot reach interesting volumes.
 */
public class BoundedTupleSpout implements IBatchSpout {

    @Serial
    private static final long serialVersionUID = 1L;

    private final long totalTuples;
    private final int batchSize;
    private final Fields outputFields;
    private final ValuesFactory valuesFactory;

    public BoundedTupleSpout(long totalTuples, int batchSize, Fields outputFields,
                             ValuesFactory valuesFactory) {
        this.totalTuples = totalTuples;
        this.batchSize = batchSize;
        this.outputFields = outputFields;
        this.valuesFactory = valuesFactory;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context) {
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        // Trident transaction ids start at MasterBatchCoordinator.INIT_TXID (1).
        long start = (batchId - MasterBatchCoordinator.INIT_TXID) * batchSize;
        if (start < 0 || start >= totalTuples) {
            return;
        }
        long end = Math.min(start + batchSize, totalTuples);
        for (long index = start; index < end; index++) {
            collector.emit(valuesFactory.create(index));
        }
    }

    @Override
    public void ack(long batchId) {
    }

    @Override
    public void close() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<>();
    }

    @Override
    public Fields getOutputFields() {
        return outputFields;
    }

    /** Builds the tuple at a given position in the sequence. */
    public interface ValuesFactory extends Serializable {
        Values create(long index);
    }
}
