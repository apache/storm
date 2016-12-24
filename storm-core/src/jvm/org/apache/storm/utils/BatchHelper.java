/**
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
package org.apache.storm.utils;

import java.util.LinkedList;
import java.util.List;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchHelper {

    private static final Logger LOG = LoggerFactory.getLogger(BatchHelper.class);

    private int batchSize = 15000;  //default batch size 15000
    private List<Tuple> tupleBatch;
    private boolean forceFlush = false;
    private OutputCollector collector;

    public BatchHelper(int batchSize, OutputCollector collector) {
        if (batchSize > 0) {
            this.batchSize = batchSize;
        }
        this.collector = collector;
        this.tupleBatch = new LinkedList<>();
    }

    public void fail(Exception e) {
        collector.reportError(e);
        for (Tuple t : tupleBatch) {
            collector.fail(t);
        }
        tupleBatch.clear();
        forceFlush = false;
    }

    public void ack() {
        for (Tuple t : tupleBatch) {
            collector.ack(t);
        }
        tupleBatch.clear();
        forceFlush = false;
    }

    public boolean shouldHandle(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            LOG.debug("TICK received! current batch status [{}/{}]", tupleBatch.size(), batchSize);
            forceFlush = true;
            return false;
        } else {
            return true;
        }
    }

    public void addBatch(Tuple tuple) {
        tupleBatch.add(tuple);
        if (tupleBatch.size() >= batchSize) {
            forceFlush = true;
        }
    }

    public List<Tuple> getBatchTuples() {
        return this.tupleBatch;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public boolean shouldFlush() {
        return forceFlush && !tupleBatch.isEmpty();
    }

}
