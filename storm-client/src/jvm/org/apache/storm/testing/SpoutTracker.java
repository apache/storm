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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.RegisteredGlobalState;


public class SpoutTracker extends BaseRichSpout {
    IRichSpout delegate;
    SpoutTrackOutputCollector tracker;
    String trackId;


    public SpoutTracker(IRichSpout delegate, String trackId) {
        this.delegate = delegate;
        this.trackId = trackId;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        tracker = new SpoutTrackOutputCollector(collector);
        delegate.open(conf, context, new SpoutOutputCollector(tracker));
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void nextTuple() {
        delegate.nextTuple();
    }

    @Override
    public void ack(Object msgId) {
        delegate.ack(msgId);
        Map<String, Object> stats = (Map<String, Object>) RegisteredGlobalState.getState(trackId);
        ((AtomicInteger) stats.get("processed")).incrementAndGet();
    }

    @Override
    public void fail(Object msgId) {
        delegate.fail(msgId);
        Map<String, Object> stats = (Map<String, Object>) RegisteredGlobalState.getState(trackId);
        ((AtomicInteger) stats.get("processed")).incrementAndGet();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    private class SpoutTrackOutputCollector implements ISpoutOutputCollector {
        public int transferred = 0;
        public int emitted = 0;
        public SpoutOutputCollector collector;

        SpoutTrackOutputCollector(SpoutOutputCollector collector) {
            this.collector = collector;
        }

        private void recordSpoutEmit() {
            Map<String, Object> stats = (Map<String, Object>) RegisteredGlobalState.getState(trackId);
            ((AtomicInteger) stats.get("spout-emitted")).incrementAndGet();

        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            List<Integer> ret = collector.emit(streamId, tuple, messageId);
            recordSpoutEmit();
            return ret;
        }

        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
            collector.emitDirect(taskId, streamId, tuple, messageId);
            recordSpoutEmit();
        }

        @Override
        public void flush() {
            collector.flush();
        }

        @Override
        public void reportError(Throwable error) {
            collector.reportError(error);
        }

        @Override
        public long getPendingCount() {
            return collector.getPendingCount();
        }

    }

}
