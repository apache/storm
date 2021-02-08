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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.storm.streams.ProcessorNode;

/**
 * A composite context that holds a chain of {@link ProcessorContext}.
 */
public class ChainedProcessorContext implements ProcessorContext {
    private final ProcessorNode processorNode;
    private final List<? extends ProcessorContext> contexts;

    public ChainedProcessorContext(ProcessorNode processorNode, List<? extends ProcessorContext> contexts) {
        this.processorNode = processorNode;
        this.contexts = new ArrayList<>(contexts);
    }

    public ChainedProcessorContext(ProcessorNode processorNode, ProcessorContext... contexts) {
        this(processorNode, Arrays.asList(contexts));
    }

    @Override
    public <T> void forward(T input) {
        for (ProcessorContext context : contexts) {
            context.forward(input);
        }
    }

    @Override
    public <T> void forward(T input, String stream) {
        for (ProcessorContext context : contexts) {
            context.forward(input, stream);
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
}
