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
package com.alibaba.jstorm.metric;

import com.alibaba.jstorm.common.metric.AsmMetric;

import java.io.Serializable;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public interface AsmMetricFilter extends Serializable {
    /**
     * Matches all metrics, regardless of type or name.
     */
    AsmMetricFilter ALL = new AsmMetricFilter() {
        private static final long serialVersionUID = 7089987006352295530L;

        @Override
        public boolean matches(String name, AsmMetric metric) {
            return true;
        }
    };

    /**
     * Returns {@code true} if the metric matches the filter; {@code false} otherwise.
     * 
     * @param name the metric node
     * @param metric the metric
     * @return {@code true} if the metric matches the filter
     */
    boolean matches(String name, AsmMetric metric);
}
