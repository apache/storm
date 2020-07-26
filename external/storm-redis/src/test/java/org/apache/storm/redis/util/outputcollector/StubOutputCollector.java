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

package org.apache.storm.redis.util.outputcollector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.tuple.Tuple;

/**
 * Stub implementation for testing.
 */
public class StubOutputCollector implements IOutputCollector {

    final List<EmittedTuple> emittedTuples = new ArrayList<>();
    final List<Tuple> ackedTuples = new ArrayList<>();
    final List<Tuple> failedTuples = new ArrayList<>();
    final List<Throwable> reportedErrors = new ArrayList<>();

    @Override
    public List<Integer> emit(final String streamId, final Collection<Tuple> anchors, final List<Object> tuple) {
        emittedTuples.add(
            new EmittedTuple(streamId, tuple, anchors)
        );

        // Dummy value.
        return Collections.singletonList(1);
    }

    @Override
    public void emitDirect(final int taskId, final String streamId, final Collection<Tuple> anchors, final List<Object> tuple) {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    public void ack(final Tuple input) {
        ackedTuples.add(input);
    }

    @Override
    public void fail(final Tuple input) {
        failedTuples.add(input);
    }

    @Override
    public void resetTimeout(final Tuple input) {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    public void flush() {
        throw new RuntimeException("Not implemented yet!");
    }

    @Override
    public void reportError(final Throwable error) {
        reportedErrors.add(error);
    }

    public List<EmittedTuple> getEmittedTuples() {
        return Collections.unmodifiableList(emittedTuples);
    }

    public List<Throwable> getReportedErrors() {
        return Collections.unmodifiableList(reportedErrors);
    }

    public List<Tuple> getFailedTuples() {
        return Collections.unmodifiableList(failedTuples);
    }

    public List<Tuple> getAckedTuples() {
        return Collections.unmodifiableList(ackedTuples);
    }

    public void reset() {
        emittedTuples.clear();
        ackedTuples.clear();
        reportedErrors.clear();
        failedTuples.clear();
    }
}
