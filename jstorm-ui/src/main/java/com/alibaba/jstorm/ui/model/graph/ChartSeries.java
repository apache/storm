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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class ChartSeries {
    private String name;
    private List<Number> data;
    private List<String> label;
    private List<String> category;


    public ChartSeries(String name) {
        this.name = name;
        data = new ArrayList<>();
        label = new ArrayList<>();
        category = new ArrayList<>();
    }

    public ChartSeries(String name, List<Number> data, List<String> label) {
        this.name = name;
        this.data = data;
        this.label = label;
    }

    public void addData(Number number){
        data.add(number);
    }

    public void addLabel(String str){
        label.add(str);
    }

    public void addCategory(String cate){
        category.add(cate);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Number> getData() {
        return data;
    }

    public void setData(List<Number> data) {
        this.data = data;
    }

    public List<String> getLabel() {
        return label;
    }

    public void setLabel(List<String> label) {
        this.label = label;
    }

    public List<String> getCategory() {
        return category;
    }

    public void setCategory(List<String> category) {
        this.category = category;
    }
}
