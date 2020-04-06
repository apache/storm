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

package org.apache.storm.loadgen;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.storm.metrics.hdrhistogram.HistogramMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * A spout that simulates a real world spout based off of statistics about it.
 */
public class LoadSpout  extends BaseRichSpout {
    private static class OutputStreamEngineWithHisto extends OutputStreamEngine {
        public final HistogramMetric histogram;

        OutputStreamEngineWithHisto(OutputStream stats, TopologyContext context) {
            super(stats);
            histogram = new HistogramMetric(3600000000000L, 3);
            //TODO perhaps we can adjust the frequency later...
            context.registerMetric("comp-lat-histo-" + stats.id, histogram, 10);
        }
    }

    private static class SentWithTime {
        public final String streamName;
        public final Values keyValue;
        public final long time;
        public final HistogramMetric histogram;

        SentWithTime(String streamName, Values keyValue, long time, HistogramMetric histogram) {
            this.streamName = streamName;
            this.keyValue = keyValue;
            this.time = time;
            this.histogram = histogram;
        }

        public void done() {
            histogram.recordValue(Math.max(0, System.nanoTime() - time));
        }
    }

    private final List<OutputStream> streamStats;
    private List<OutputStreamEngineWithHisto> streams;
    private SpoutOutputCollector collector;
    //This is an attempt to give all of the streams an equal opportunity to emit something.
    private long nextStreamCounter = 0;
    private final int numStreams;
    private final Queue<SentWithTime> replays = new ArrayDeque<>();

    /**
     * Create a simple load spout with just a set rate per second on the default stream.
     * @param ratePerSecond the rate to send messages at.
     */
    public LoadSpout(double ratePerSecond) {
        OutputStream test = new OutputStream.Builder()
            .withId("default")
            .withRate(new NormalDistStats(ratePerSecond, 0.0, ratePerSecond, ratePerSecond))
            .build();
        streamStats = Arrays.asList(test);
        numStreams = 1;
    }

    public LoadSpout(LoadCompConf conf) {
        this.streamStats = Collections.unmodifiableList(new ArrayList<>(conf.streams));
        numStreams = streamStats.size();
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        streams = Collections.unmodifiableList(streamStats.stream()
            .map((ss) -> new OutputStreamEngineWithHisto(ss, context)).collect(Collectors.toList()));
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (!replays.isEmpty()) {
            SentWithTime swt = replays.poll();
            collector.emit(swt.streamName, swt.keyValue, swt);
            return;
        }
        int size = numStreams;
        for (int tries = 0; tries < size; tries++) {
            int index = Math.abs((int) (nextStreamCounter++ % size));
            OutputStreamEngineWithHisto se = streams.get(index);
            Long emitTupleTime = se.shouldEmit();
            if (emitTupleTime != null) {
                SentWithTime swt =
                    new SentWithTime(se.streamName, getNextValues(se), emitTupleTime, se.histogram);
                collector.emit(swt.streamName, swt.keyValue, swt);
                break;
            }
        }
    }

    protected Values getNextValues(OutputStreamEngine se) {
        return new Values(se.nextKey(), "JUST_SOME_VALUE");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (OutputStream s: streamStats) {
            declarer.declareStream(s.id, new Fields("key", "value"));
        }
    }

    @Override
    public void ack(Object id) {
        ((SentWithTime) id).done();
    }

    @Override
    public void fail(Object id) {
        replays.add((SentWithTime) id);
    }
}
