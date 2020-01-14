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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.RegisteredGlobalState;


public class NonRichBoltTracker implements IBolt {
    private static final long serialVersionUID = -9005809961297778879L;
    IBolt delegate;
    String trackId;

    public NonRichBoltTracker(IBolt delegate, String id) {
        this.delegate = delegate;
        trackId = id;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        delegate.prepare(topoConf, context, collector);
    }

    @Override
    public void execute(Tuple input) {
        delegate.execute(input);
        Map<String, Object> stats = (Map<String, Object>) RegisteredGlobalState.getState(trackId);
        ((AtomicInteger) stats.get("processed")).incrementAndGet();
    }

    @Override
    public void cleanup() {
        delegate.cleanup();
    }
}
