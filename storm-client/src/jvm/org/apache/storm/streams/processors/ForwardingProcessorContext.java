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

import java.util.Set;
import org.apache.storm.shade.com.google.common.collect.Multimap;
import org.apache.storm.streams.ProcessorNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A context that emits the results to downstream processors which are in the same bolt.
 */
public class ForwardingProcessorContext implements ProcessorContext {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardingProcessorContext.class);
    private final ProcessorNode processorNode;
    private final Multimap<String, ProcessorNode> streamToChildren;
    private final Set<String> streams;

    public ForwardingProcessorContext(ProcessorNode processorNode, Multimap<String, ProcessorNode> streamToChildren) {
        this.processorNode = processorNode;
        this.streamToChildren = streamToChildren;
        this.streams = streamToChildren.keySet();
    }

    @Override
    public <T> void forward(T input) {
        if (PUNCTUATION.equals(input)) {
            finishAllStreams();
        } else {
            executeAllStreams(input);
        }
    }

    @Override
    public <T> void forward(T input, String stream) {
        if (PUNCTUATION.equals(input)) {
            finish(stream);
        } else {
            execute(input, stream);
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

    private void finishAllStreams() {
        for (String stream : streams) {
            finish(stream);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void finish(String stream) {
        for (ProcessorNode node : streamToChildren.get(stream)) {
            LOG.debug("Punctuating processor: {}", node);
            Processor<T> processor = (Processor<T>) node.getProcessor();
            processor.punctuate(stream);
        }
    }

    private <T> void executeAllStreams(T input) {
        for (String stream : streams) {
            execute(input, stream);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void execute(T input, String stream) {
        for (ProcessorNode node : streamToChildren.get(stream)) {
            LOG.debug("Forward input: {} to processor node: {}", input, node);
            Processor<T> processor = (Processor<T>) node.getProcessor();
            processor.execute(input, stream);
        }
    }
}
