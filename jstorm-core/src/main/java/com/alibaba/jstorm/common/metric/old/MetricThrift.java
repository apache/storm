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
package com.alibaba.jstorm.common.metric.old;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricWindow;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MetricThrift {
    private static final Logger LOG = LoggerFactory.getLogger(MetricThrift.class);

    public static MetricInfo mkMetricInfo() {
        return new MetricInfo();
    }

    public static void insert(MetricInfo metricInfo, String key, Map<Integer, Double> windowSet) {
    }

    public static MetricWindow merge(Map<String, MetricWindow> details) {
        Map<Integer, Double> merge = new HashMap<Integer, Double>();

        for (Entry<String, MetricWindow> entry : details.entrySet()) {
            MetricWindow metricWindow = entry.getValue();
            Map<Integer, Double> metric = metricWindow.get_metricWindow();

            for (Entry<Integer, Double> metricEntry : metric.entrySet()) {
                Integer key = metricEntry.getKey();
                try {
                    Double value = ((Number) JStormUtils.add(metricEntry.getValue(), merge.get(key))).doubleValue();
                    merge.put(key, value);
                } catch (Exception e) {
                    LOG.error("Invalid type of " + entry.getKey() + ":" + key, e);
                }
            }
        }

        MetricWindow ret = new MetricWindow();

        ret.set_metricWindow(merge);
        return ret;
    }

    public static void merge(MetricInfo metricInfo, Map<String, Map<String, MetricWindow>> extraMap) {
        for (Entry<String, Map<String, MetricWindow>> entry : extraMap.entrySet()) {
            String metricName = entry.getKey();
            // metricInfo.put_to_baseMetric(metricName, merge(entry.getValue()));
        }
    }

    public static MetricWindow mergeMetricWindow(MetricWindow fromMetric, MetricWindow toMetric) {
        if (toMetric == null) {
            toMetric = new MetricWindow(new HashMap<Integer, Double>());
        }

        if (fromMetric == null) {
            return toMetric;
        }

        List<Map<Integer, Double>> list = new ArrayList<Map<Integer, Double>>();
        list.add(fromMetric.get_metricWindow());
        list.add(toMetric.get_metricWindow());
        Map<Integer, Double> merged = JStormUtils.mergeMapList(list);

        toMetric.set_metricWindow(merged);

        return toMetric;
    }

    public static MetricInfo mergeMetricInfo(MetricInfo from, MetricInfo to) {
        if (to == null) {
            to = mkMetricInfo();
        }

        if (from == null) {
            return to;
        }
        // to.get_baseMetric().putAll(from.get_baseMetric());

        return to;

    }

}
