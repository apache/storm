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
package com.alibaba.jstorm.ui.model.graph;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class TopologyEdge {
    int id;
    String from;
    String to;
    double value;
    String title;
    double cycleValue;
    boolean isHidden;
    Map<String, Map<Integer, String>> mapValue = new HashMap<>();


    public TopologyEdge(String from, String to, int id) {
        this.from = from;
        this.to = to;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void putMapValue(String metricName, int window, String number) {
        Map<Integer, String> winData = mapValue.get(metricName);
        if (winData == null) {
            winData = new HashMap<>();
            mapValue.put(metricName, winData);
        }
        winData.put(window, number);
    }

    public Map<String, Map<Integer, String>> getMapValue() {
        return mapValue;
    }

    public void setMapValue(Map<String, Map<Integer, String>> mapValue) {
        this.mapValue = mapValue;
    }

    public double getCycleValue() {
        return cycleValue;
    }

    public void setCycleValue(double cycleValue) {
        this.cycleValue = cycleValue;
    }

    public void appendTitle(String title) {
        if (StringUtils.isEmpty(this.title)) {
            this.title = title;
        } else {
            this.title += "<br/>" + title;
        }
    }


    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public boolean isHidden() {
        return isHidden;
    }

    public void setIsHidden(boolean isHidden) {
        this.isHidden = isHidden;
    }

    public String getKey(){
        return from + ":" + to;
    }
}
