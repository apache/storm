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
package com.alibaba.jstorm.ui.utils;

import backtype.storm.generated.MetricSnapshot;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.ui.model.UIBasicMetric;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UIMetricUtils {

    public static final DecimalFormat format = new DecimalFormat(",###.##");


    public static List<String> sortHead(List<? extends UIBasicMetric> list, String[] HEAD) {
        List<String> sortedHead = new ArrayList<>();
        Set<String> keys = new HashSet<>();
        for (UIBasicMetric metric : list) {
            keys.addAll(metric.getMetrics().keySet());
        }
        for (String h : HEAD) {
            if (keys.contains(h)) {
                sortedHead.add(h);
                keys.remove(h);
            }
        }
        sortedHead.addAll(keys);
        return sortedHead;
    }

    public static List<String> sortHead(UIBasicMetric metric, String[] HEAD) {
        List<String> sortedHead = new ArrayList<>();
        if (metric == null) return sortedHead;
        Set<String> keys = new HashSet<>();
        keys.addAll(metric.getMetrics().keySet());
        for (String h : HEAD) {
            if (keys.contains(h)) {
                sortedHead.add(h);
                keys.remove(h);
            }
        }
        sortedHead.addAll(keys);
        return sortedHead;
    }

    /**
     * get MetricSnapshot formatted value string
     */
    public static String getMetricValue(MetricSnapshot snapshot) {
        if (snapshot == null) return null;
        MetricType type = MetricType.parse(snapshot.get_metricType());
        switch (type) {
            case COUNTER:
                return format(snapshot.get_longValue());
            case GAUGE:
                return format(snapshot.get_doubleValue());
            case METER:
                return format(snapshot.get_m1());
            case HISTOGRAM:
                return format(snapshot.get_mean());
            case TIMER:
                return format(snapshot.get_mean());
            default:
                return "0";
        }
    }

    public static String getMetricRawValue(MetricSnapshot snapshot) {
        MetricType type = MetricType.parse(snapshot.get_metricType());
        switch (type) {
            case COUNTER:
                return snapshot.get_longValue() + "";
            case GAUGE:
                return snapshot.get_doubleValue() + "";
            case METER:
                return snapshot.get_m1() + "";
            case HISTOGRAM:
                return snapshot.get_mean() + "";
            case TIMER:
                return snapshot.get_mean() + "";
            default:
                return "0";
        }
    }

    public static Number getMetricNumberValue(MetricSnapshot snapshot){
        MetricType type = MetricType.parse(snapshot.get_metricType());
        switch (type) {
            case COUNTER:
                return snapshot.get_longValue();
            case GAUGE:
                return snapshot.get_doubleValue();
            case METER:
                return snapshot.get_m1();
            case HISTOGRAM:
                return snapshot.get_mean();
            case TIMER:
                return snapshot.get_mean();
            default:
                return 0;
        }
    }

    public static String format(double value) {
        return format.format(value);
    }

    public static String format(double value, String f){
        DecimalFormat _format = new DecimalFormat(f);
        return _format.format(value);
    }

    public static String format(long value) {
        return format.format(value);
    }


    // Extract compName from 'CC@SequenceTest4-1-1439469823@Merge@0@@sys@Emitted',which is 'Merge'
    public static String extractComponentName(String[] strs) {
        if (strs.length < 6) return null;
        return strs[2];
    }

    // Extract TaskId from 'TH@SequenceTest2-2-1439865106@Split@17@@sys@SerializeTime',which is '17'
    public static String extractTaskId(String[] strs) {
        if (strs.length < 6) return null;
        return strs[3];
    }

    // Extract StreamId from 'SH@SequenceTest2-2-1439865106@SequenceSpout@35@default@sys@ProcessLatency',which is 'default'
    public static String extractStreamId(String[] strs) {
        if (strs.length < 6) return null;
        return strs[4];
    }

    // Extract Group from 'WM@SequenceTest2-2-1439865106@10.218.132.134@6800@sys@MemUsed',which is 'sys'
    public static String extractGroup(String[] strs) {
        if (strs.length < 6) return null;
        return strs[strs.length - 2];
    }

    // Extract MetricName from 'CC@SequenceTest4-1-1439469823@Merge@0@@sys@Emitted',which is 'Emitted'
    public static String extractMetricName(String[] strs) {
        if (strs.length < 6) return null;
        return strs[strs.length - 1];
    }
}
