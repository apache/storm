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

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class TopologyNode {
    String id;
    double value;
    String label;
    boolean isSpout;
    boolean isHidden;
    String title;
    int level;
    Map<String, Map<Integer, String>> mapValue = new HashMap<>();

    public TopologyNode(String id, String label, boolean isSpout) {
        this.id = id;
        this.label = label;
        this.isSpout = isSpout;
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public boolean isSpout() {
        return isSpout;
    }

    public void setIsSpout(boolean isSpout) {
        this.isSpout = isSpout;
    }

    public String getTitle() {
        return title;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public boolean isHidden() {
        return isHidden;
    }

    public void setIsHidden(boolean isHidden) {
        this.isHidden = isHidden;
    }
}