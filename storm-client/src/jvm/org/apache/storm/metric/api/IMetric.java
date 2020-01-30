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

package org.apache.storm.metric.api;

import java.util.Collections;
import java.util.Map;

/**
 * Produces metrics.
 * Usually, metric is a measurement identified by a name string.
 * Dimensions are a collection of additional key-value metadata map containing extra information of this measurement.
 * It is optional when customizing your metric by implementing this interface
 */
public interface IMetric {
    /**
     * Get value and reset.
     *
     * @return an object that will be sent to
     *     {@link IMetricsConsumer#handleDataPoints(org.apache.storm.metric.api.IMetricsConsumer.TaskInfo, java.util.Collection)}.
     *     If {@code null} is returned nothing will be sent. If this value can be reset, like with a counter, a side effect
     *     of calling this should be that the value is reset.
     */
    Object getValueAndReset();


    /**
     * Check whether this metric is carrying additional dimension map.
     * @return a boolean value.
     */
    default boolean isDimensional() {
        return false;
    }

    /**
     * Get dimension map. An empty map will be returned if metric is not dimensional.
     * @return a K-V map of the additional metadata.
     */
    default Map<String, String> getDimensions() {
        return Collections.emptyMap();
    }
}
