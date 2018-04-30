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
import java.util.Set;
import org.apache.storm.annotation.InterfaceStability;

/**
 * Context information passed to the {@link Processor}.
 */
@InterfaceStability.Unstable
public interface ProcessorContext extends Serializable {
    /**
     * Forwards the input to all downstream processors.
     *
     * @param input the input
     * @param <T>   the type of the input
     */
    <T> void forward(T input);

    /**
     * Forwards the input to downstream processors at specified stream.
     *
     * @param input  the input
     * @param stream the stream to forward
     * @param <T>    the type of the input
     */
    <T> void forward(T input, String stream);

    /**
     * Returns true if the processing is in a windowed context and should wait for punctuation before emitting results.
     *
     * @return whether this is a windowed context or not
     */
    boolean isWindowed();

    /**
     * Returns the windowed parent streams. These are the streams where punctuations arrive.
     *
     * @return the windowed parent streams
     */
    Set<String> getWindowedParentStreams();
}
