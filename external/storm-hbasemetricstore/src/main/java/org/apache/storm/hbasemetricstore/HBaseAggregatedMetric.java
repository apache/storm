/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.hbasemetricstore;

import org.apache.storm.metricstore.Metric;

/**
 * Class that adds a version field to a Metric.  Aggregated metrics may be updated in HBase at the same time by two
 * different threads.  The metric can be conditionally updated in HBase by checking that the version matches the
 * expected value.  If the check fails, the metric can be re-fetched and re-aggregated and then updated on a retry.
 */
class HBaseAggregatedMetric extends Metric {
    private int version;

    /**
     * A Metric constructor with the same settings cloned from another.
     *
     * @param o   metric to clone
     */
    HBaseAggregatedMetric(Metric o) {
        super(o);
        this.version = 0;
    }

    void setVersion(int version) {
        this.version = version;
    }

    int getVersion() {
        return this.version;
    }
}
