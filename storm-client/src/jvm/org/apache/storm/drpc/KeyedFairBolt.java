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

package org.apache.storm.drpc;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.coordination.CoordinatedBolt.FinishedCallback;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicBoltExecutor;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.KeyedRoundRobinQueue;


public class KeyedFairBolt implements IRichBolt, FinishedCallback {
    IRichBolt delegate;
    KeyedRoundRobinQueue<Tuple> rrQueue;
    Thread executor;
    FinishedCallback callback;

    public KeyedFairBolt(IRichBolt delegate) {
        this.delegate = delegate;
    }

    public KeyedFairBolt(IBasicBolt delegate) {
        this(new BasicBoltExecutor(delegate));
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        if (delegate instanceof FinishedCallback) {
            callback = (FinishedCallback) delegate;
        }
        delegate.prepare(topoConf, context, collector);
        rrQueue = new KeyedRoundRobinQueue<Tuple>();
        executor = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        delegate.execute(rrQueue.take());
                    }
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        });
        executor.setDaemon(true);
        executor.start();
    }

    @Override
    public void execute(Tuple input) {
        Object key = input.getValue(0);
        rrQueue.add(key, input);
    }

    @Override
    public void cleanup() {
        executor.interrupt();
        delegate.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public void finishedId(Object id) {
        if (callback != null) {
            callback.finishedId(id);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }
}
