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
package com.alibaba.jstorm.ui.model;

import backtype.storm.generated.MetricSnapshot;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UISummaryMetric extends UIBasicMetric {

    public static final String[] HEAD = {MetricDef.FAILED_NUM, MetricDef.EMMITTED_NUM, MetricDef.ACKED_NUM, MetricDef.SEND_TPS,
            MetricDef.RECV_TPS, MetricDef.PROCESS_LATENCY};

    public void setMetricValue(MetricSnapshot snapshot, String metricName) {
        if (metricName.equals(MetricDef.MEMORY_USED)) {
            String value = (long) snapshot.get_doubleValue() + "";
            setValue(metricName, value);

        } else {
            String value = UIMetricUtils.getMetricValue(snapshot);
            setValue(metricName, value);
        }
    }

}
