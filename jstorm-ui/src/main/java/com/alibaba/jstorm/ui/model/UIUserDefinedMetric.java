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

import com.alibaba.jstorm.metric.MetricType;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class UIUserDefinedMetric {
    protected String metricName;
    protected String componentName;
    protected String componentType;
    protected String type;

    protected String value;

    public UIUserDefinedMetric(String metricName, String componentName) {
        this.metricName = metricName;
        this.componentName = componentName;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    public String getComponentType() {
        return componentType;
    }

    public void setComponentType(String componentType) {
        this.componentType = componentType;
    }

    public String getType() {
        return type;
    }

    public void setType(int type) {
        MetricType metricType = MetricType.parse(type);
        switch (metricType) {
            case COUNTER:
                this.type = "Counter";
            case GAUGE:
                this.type = "Gauge";
            case METER:
                this.type = "Meter";
            case HISTOGRAM:
                this.type = "Histogram";
            case TIMER:
                this.type = "Timer";
        }
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
