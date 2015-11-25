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

import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.MetricSnapshot;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;
import com.alibaba.jstorm.utils.JStormUtils;

import java.util.*;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UIComponentMetric extends UIBasicMetric {
    protected String componentName;
    protected int parallel;
    protected String type;
    protected List<ErrorInfo> errors;
    protected String sortedKey;

    // <metricName, <parentCompName, value>>
    protected Map<String, Map<String, String>> subMetricMap = new HashMap<>();
    // <metricName@parentCompName, value>, this is used to iterate in jsp
    protected Map<String, String> subMetrics = new HashMap<>();
    protected Set<String> parentComponent;

    public static final String[] HEAD = {MetricDef.EMMITTED_NUM, MetricDef.ACKED_NUM, MetricDef.FAILED_NUM, MetricDef.SEND_TPS,
            MetricDef.RECV_TPS, MetricDef.PROCESS_LATENCY, MetricDef.DESERIALIZE_TIME, MetricDef.SERIALIZE_TIME,
            MetricDef.EXECUTE_TIME, MetricDef.COLLECTOR_EMIT_TIME, MetricDef.TUPLE_LIEF_CYCLE, MetricDef.ACKER_TIME};

    public UIComponentMetric(String componentName) {
        this.componentName = componentName;
    }

    public UIComponentMetric(String componentName, int parallel, String type) {
        this.componentName = componentName;
        this.parallel = parallel;
        this.type = type;
    }

    public void setMetricValue(MetricSnapshot snapshot, String parentComp, String metricName) {
        if (parentComp != null) {
            String value = UIMetricUtils.getMetricRawValue(snapshot);
            putSubMetricMap(metricName, parentComp, value);
        } else {
            String value = UIMetricUtils.getMetricValue(snapshot);
            setValue(metricName, value);
        }
    }


    public void mergeValue() {
        Set<String> set = new HashSet<>();
        for (Map.Entry<String, Map<String, String>> entry : subMetricMap.entrySet()) {
            String metricName = entry.getKey();
//            String compName = entry.getKey().split("@")[1];
            if (getValue(metricName) == null) {
                // only when the value is not be set yet, we set the merged value
                String value = merge(metricName, entry.getValue());
                setValue(metricName, value);
            }
            //add all the component names to the set
            set.addAll(entry.getValue().keySet());
            // fill the subMetrics , format <metricName@parentCompName, value>
            for(Map.Entry<String, String> en : entry.getValue().entrySet()){
                String compName = en.getKey();
                String v = en.getValue();
                String value;
                if(v.contains(".")){
                    value = UIMetricUtils.format(JStormUtils.parseDouble(v));
                }else{
                    value = UIMetricUtils.format(JStormUtils.parseLong(v));
                }
                subMetrics.put(metricName+"@"+compName, value);
            }
        }
        parentComponent = set;
    }

    protected String merge(String metricName, Map<String, String> list) {
        double value = 0d;

        if (metricName.equals(MetricDef.EMMITTED_NUM) || metricName.equals(MetricDef.ACKED_NUM)
                || metricName.equals(MetricDef.FAILED_NUM)) {
            for (String s : list.values()) {
                value += JStormUtils.parseDouble(s);
            }
            return UIMetricUtils.format((long) value);
        } else {
            for (String s : list.values()) {
                value += JStormUtils.parseDouble(s);
            }
            if (list.size() > 0) value = value / list.size();
            return UIMetricUtils.format(value);
        }
    }


    public void putSubMetricMap(String metricName, String parentComp, String value) {
//        String key = metricName + "@" + parentComp;
//        subMetricMap.put(key, value);
        if (subMetricMap.containsKey(metricName)) {
            Map<String, String> map = subMetricMap.get(metricName);
            map.put(parentComp, value);
        } else {
            Map<String, String> map = new HashMap<>();
            map.put(parentComp, value);
            subMetricMap.put(metricName, map);
        }
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getComponentName() {
        return componentName;
    }

    public int getParallel() {
        return parallel;
    }

    public String getType() {
        return type;
    }


    public Map<String, String> getSubMetrics() {
        return subMetrics;
    }

    public Set<String> getParentComponent() {
        return parentComponent;
    }

    public List<ErrorInfo> getErrors() {
        return errors;
    }

    public void setErrors(List<ErrorInfo> errors) {
        this.errors = errors;
    }

    public String getSortedKey() {
        return sortedKey;
    }

    public void setSortedKey(String sortedKey) {
        this.sortedKey = sortedKey;
    }
}
