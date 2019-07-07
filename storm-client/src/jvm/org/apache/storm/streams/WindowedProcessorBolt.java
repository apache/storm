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

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.storm.shade.com.google.common.collect.Multimap;
import org.apache.storm.shade.org.jgrapht.DirectedGraph;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream bolt that executes windowing operations.
 */
class WindowedProcessorBolt extends BaseWindowedBolt implements StreamBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WindowedProcessorBolt.class);
    private final ProcessorBoltDelegate delegate;
    private final Window<?, ?> window;

    WindowedProcessorBolt(String id, DirectedGraph<Node, Edge> graph,
                          List<ProcessorNode> nodes,
                          Window<?, ?> window) {
        delegate = new ProcessorBoltDelegate(id, graph, nodes);
        this.window = window;
        setWindowConfig();
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        delegate.prepare(topoConf, context, collector);
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        LOG.trace("Window triggered at {}, inputWindow {}", new Date(), inputWindow);
        if (delegate.isEventTimestamp()) {
            delegate.setEventTimestamp(inputWindow.getEndTimestamp());
        }
        for (Tuple tuple : inputWindow.get()) {
            Pair<Object, String> valueAndStream = delegate.getValueAndStream(tuple);
            if (!StreamUtil.isPunctuation(valueAndStream.getFirst())) {
                delegate.process(valueAndStream.getFirst(), valueAndStream.getSecond());
            }
        }
        for (String stream : delegate.getInitialStreams()) {
            delegate.process(PUNCTUATION, stream);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public void setTimestampField(String fieldName) {
        delegate.setTimestampField(fieldName);
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    private void setWindowConfig() {
        if (window instanceof SlidingWindows) {
            setSlidingWindowParams(window.getWindowLength(), window.getSlidingInterval());
        } else if (window instanceof TumblingWindows) {
            setTumblingWindowParams(window.getWindowLength());
        }
        if (window.getTimestampField() != null) {
            withTimestampField(window.getTimestampField());
        }
        if (window.getLag() != null) {
            withLag(window.getLag());
        }
        if (window.getLateTupleStream() != null) {
            withLateTupleStream(window.getLateTupleStream());
        }
    }

    private void setSlidingWindowParams(Object windowLength, Object slidingInterval) {
        if (windowLength instanceof Count) {
            if (slidingInterval instanceof Count) {
                withWindow((Count) windowLength, (Count) slidingInterval);
            } else if (slidingInterval instanceof Duration) {
                withWindow((Count) windowLength, (Duration) slidingInterval);
            }
        } else if (windowLength instanceof Duration) {
            if (slidingInterval instanceof Count) {
                withWindow((Duration) windowLength, (Count) slidingInterval);
            } else if (slidingInterval instanceof Duration) {
                withWindow((Duration) windowLength, (Duration) slidingInterval);
            }
        }
    }

    private void setTumblingWindowParams(Object windowLength) {
        if (windowLength instanceof Count) {
            withTumblingWindow((Count) windowLength);
        } else if (windowLength instanceof Duration) {
            withTumblingWindow((Duration) windowLength);
        }
    }

    void setStreamToInitialProcessors(Multimap<String, ProcessorNode> streamToInitialProcessors) {
        delegate.setStreamToInitialProcessors(streamToInitialProcessors);
    }
}
