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

package org.apache.storm.trident;

import org.apache.storm.generated.SharedMemory;
import org.apache.storm.topology.ResourceDeclarer;
import org.apache.storm.trident.planner.Node;


public class TridentState implements ResourceDeclarer<TridentState> {
    TridentTopology topology;
    Node node;

    protected TridentState(TridentTopology topology, Node node) {
        this.topology = topology;
        this.node = node;
    }

    public Stream newValuesStream() {
        return new Stream(topology, node.name, node);
    }

    public TridentState parallelismHint(int parallelism) {
        node.parallelismHint = parallelism;
        return this;
    }

    @Override
    public TridentState setCPULoad(Number load) {
        node.setCPULoad(load);
        return this;
    }

    @Override
    public TridentState setMemoryLoad(Number onHeap) {
        node.setMemoryLoad(onHeap);
        return this;
    }

    @Override
    public TridentState setMemoryLoad(Number onHeap, Number offHeap) {
        node.setMemoryLoad(onHeap, offHeap);
        return this;
    }

    @Override
    public TridentState addSharedMemory(SharedMemory request) {
        node.addSharedMemory(request);
        return this;
    }
}
