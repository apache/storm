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

package org.apache.storm.streams.processors;

import static org.apache.storm.streams.WindowNode.PUNCTUATION;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.ProcessorNode;
import org.apache.storm.streams.RefCountedTuple;
import org.apache.storm.streams.StreamUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A context that emits the results to downstream processors which are in another bolt.
 */
public class EmittingProcessorContext implements ProcessorContext {
    private static final Logger LOG = LoggerFactory.getLogger(EmittingProcessorContext.class);
    private final ProcessorNode processorNode;
    private final String outputStreamId;
    private final String punctuationStreamId;
    private final OutputCollector collector;
    private final Fields outputFields;
    private final Values punctuation;
    private final List<RefCountedTuple> anchors = new ArrayList<>();
    private long eventTimestamp;
    private String timestampField;

    public EmittingProcessorContext(ProcessorNode processorNode, OutputCollector collector, String outputStreamId) {
        this.processorNode = processorNode;
        this.outputStreamId = outputStreamId;
        this.collector = collector;
        outputFields = processorNode.getOutputFields();
        punctuation = new Values(PUNCTUATION);
        punctuationStreamId = StreamUtil.getPunctuationStream(outputStreamId);
    }

    @Override
    public <T> void forward(T input) {
        if (PUNCTUATION.equals(input)) {
            emit(punctuation, punctuationStreamId);
            maybeAck();
        } else if (processorNode.emitsPair()) {
            Pair<?, ?> value = (Pair<?, ?>) input;
            emit(new Values(value.getFirst(), value.getSecond()), outputStreamId);
        } else {
            emit(new Values(input), outputStreamId);
        }
    }

    @Override
    public <T> void forward(T input, String stream) {
        if (stream.equals(outputStreamId)) {
            forward(input);
        }
    }

    @Override
    public boolean isWindowed() {
        return processorNode.isWindowed();
    }

    @Override
    public Set<String> getWindowedParentStreams() {
        return processorNode.getWindowedParentStreams();
    }

    public void setTimestampField(String fieldName) {
        timestampField = fieldName;
    }

    public void setAnchor(RefCountedTuple anchor) {
        if (processorNode.isWindowed() && processorNode.isBatch()) {
            anchor.increment();
            anchors.add(anchor);
        } else {
            if (anchors.isEmpty()) {
                anchors.add(anchor);
            } else {
                anchors.set(0, anchor);
            }
            /*
             * track punctuation in non-batch mode so that the
             * punctuation is acked after all the processors have emitted the punctuation downstream.
             */
            if (StreamUtil.isPunctuation(anchor.tuple().getValue(0))) {
                anchor.increment();
            }
        }
    }

    public void setEventTimestamp(long timestamp) {
        this.eventTimestamp = timestamp;
    }

    private void maybeAck() {
        if (!anchors.isEmpty()) {
            for (RefCountedTuple anchor : anchors) {
                anchor.decrement();
                if (anchor.shouldAck()) {
                    LOG.debug("Acking {} ", anchor);
                    collector.ack(anchor.tuple());
                    anchor.setAcked();
                }
            }
            anchors.clear();
        }
    }

    private Collection<Tuple> tuples(Collection<RefCountedTuple> anchors) {
        return anchors.stream().map(RefCountedTuple::tuple).collect(Collectors.toList());
    }

    private void emit(Values values, String outputStreamId) {
        if (timestampField != null) {
            values.add(eventTimestamp);
        }
        if (anchors.isEmpty()) {
            // for windowed bolt, windowed output collector will do the anchoring/acking
            LOG.debug("Emit un-anchored, outputStreamId: {}, values: {}", outputStreamId, values);
            collector.emit(outputStreamId, values);
        } else {
            LOG.debug("Emit, outputStreamId: {}, anchors: {}, values: {}", outputStreamId, anchors, values);
            collector.emit(outputStreamId, tuples(anchors), values);
        }
    }
}
