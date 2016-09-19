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
package org.apache.storm.streams;

import org.apache.storm.streams.processors.BatchProcessor;
import org.apache.storm.streams.processors.Processor;
import org.apache.storm.streams.processors.ProcessorContext;
import org.apache.storm.tuple.Fields;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Node that wraps a processor in the stream.
 */
public class ProcessorNode extends Node {
    private final Processor<?> processor;
    private final boolean isBatch;
    private boolean windowed;
    // Windowed parent streams
    private Set<String> windowedParentStreams = Collections.emptySet();

    public ProcessorNode(Processor<?> processor, String outputStream, Fields outputFields) {
        super(outputStream, outputFields);
        this.isBatch = processor instanceof BatchProcessor;
        this.processor = processor;
    }

    public Processor<?> getProcessor() {
        return processor;
    }

    public boolean isWindowed() {
        return windowed;
    }

    public boolean isBatch() {
        return isBatch;
    }

    public void setWindowed(boolean windowed) {
        this.windowed = windowed;
    }

    public Set<String> getWindowedParentStreams() {
        return Collections.unmodifiableSet(windowedParentStreams);
    }

    void initProcessorContext(ProcessorContext context) {
        processor.init(context);
    }

    void setWindowedParentStreams(Set<String> windowedParentStreams) {
        this.windowedParentStreams = new HashSet<>(windowedParentStreams);
    }

    @Override
    public String toString() {
        return "ProcessorNode{" +
                "processor=" + processor +
                ", windowed=" + windowed +
                ", windowedParentStreams=" + windowedParentStreams +
                "} " + super.toString();
    }
}
