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

package org.apache.storm.pmml.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.storm.pmml.PMMLPredictorBolt;
import org.apache.storm.tuple.Fields;

/**
 * Represents the streams and output fields declared by the {@link PMMLPredictorBolt}.
 */
public interface ModelOutputs extends Serializable {

    /**
     * Stream fields.
     * @return a map with the output fields declared for each stream by the {@link PMMLPredictorBolt}
     */
    Map<String, ? extends Fields> streamFields();

    /**
     * Convenience method that returns a set with all the streams declared by the {@link PMMLPredictorBolt}.
     * By default this this method calls {@link #streamFields()}{@code .keySet()}.
     * @return The set with all declared streams
     */
    default Set<String> streams() {
        return streamFields().keySet();
    }
}
