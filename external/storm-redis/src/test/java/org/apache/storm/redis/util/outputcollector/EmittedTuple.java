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

import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Used with StubOutputCollector for testing.
 */
public class EmittedTuple {
    private final String streamId;
    private final List<Object> tuple;
    private final List<Tuple> anchors;

    public EmittedTuple(final String streamId, final List<Object> tuple, final Collection<Tuple> anchors) {
        this.streamId = streamId;
        this.tuple = tuple;
        this.anchors = new ArrayList<>(anchors);
    }

    public String getStreamId() {
        return streamId;
    }

    public List<Object> getTuple() {
        return tuple;
    }

    public List<Tuple> getAnchors() {
        return Collections.unmodifiableList(anchors);
    }

    @Override
    public String toString() {
        return "EmittedTuple{"
            + "streamId='" + streamId + '\''
            + ", tuple=" + tuple
            + ", anchors=" + anchors
            + '}';
    }
}
