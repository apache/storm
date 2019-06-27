/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.pmml.runner;

import java.util.List;
import java.util.Map;

import org.apache.storm.pmml.model.ModelOutputs;
import org.apache.storm.tuple.Tuple;

public interface ModelRunner {

    /**
     * Creates and returns a map with the predicted scores that are to be emitted on each stream.
     * The keys of this map are the stream ids, and the values the predicted scores.
     * It's up to the implementation to guarantee that the streams ids match the stream ids defined in
     * {@link ModelOutputs}. Namely, the set of keys of the {@code Map<String, List<Object>>} returned
     * by this method should be a subset of {@link ModelOutputs#streams()}
     *
     * @return The map with the predicted scores that are to be emitted on each stream
     */
    Map<String, List<Object>> scoredTuplePerStream(Tuple input);
}
