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

import java.io.Serializable;
import org.apache.storm.annotation.InterfaceStability;

/**
 * A processor processes a stream of elements and produces some result.
 *
 * @param <T> the type of the input that is processed
 */
@InterfaceStability.Unstable
public interface Processor<T> extends Serializable {
    /**
     * Initializes the processor. This is typically invoked from the underlying storm bolt's prepare method.
     *
     * @param context the processor context
     */
    void init(ProcessorContext context);

    /**
     * Executes some operations on the input and possibly emits some results.
     *
     * @param input    the input to be processed
     * @param streamId the source stream id from where the input is received
     */
    void execute(T input, String streamId);

    /**
     * Punctuation marks end of a batch which can be used to compute and pass the results of one stage in the pipeline to the next. For e.g.
     * emit the results of an aggregation.
     *
     * @param stream the stream id on which the punctuation arrived
     */
    void punctuate(String stream);
}
