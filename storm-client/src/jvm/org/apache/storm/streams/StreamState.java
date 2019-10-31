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

package org.apache.storm.streams;

import java.io.Serializable;

/**
 * A wrapper for the stream state which can be used to query the state via {@link Stream#stateQuery(StreamState)}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class StreamState<K, V> implements Serializable {
    private final transient PairStream<K, V> stream;

    StreamState(PairStream<K, V> stream) {
        this.stream = stream;
    }

    public PairStream<K, V> toPairStream() {
        return stream;
    }

    ProcessorNode getUpdateStateNode() {
        return (ProcessorNode) stream.node;
    }
}
