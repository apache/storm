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
package com.alibaba.jstorm.ui.model.pages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.common.metric.window.StatBuckets;
import com.alibaba.jstorm.ui.UIDef;
import com.alibaba.jstorm.ui.model.LinkData;

/**
 * 
 * @author xin.zhou/Longda
 */
@ManagedBean(name = "windowtablepage")
@ViewScoped
public class WindowTablePage extends TablePage {
    private static final Logger LOG = LoggerFactory
            .getLogger(WindowTablePage.class);
    protected String windowsTitle;
    protected List<LinkData> windowLinks = new ArrayList<LinkData>();

    public WindowTablePage() throws Exception {
        super();
        initWindowLinks();
    }

    public WindowTablePage(Map<String, String> parameterMap) throws Exception {
        super(parameterMap);

        initWindowLinks();
    }

    public void initWindowLinks() {
        windowsTitle = "JStorm Time Windows";
        for (Integer timeWindow : StatBuckets.TIME_WINDOWS) {
            LinkData link = new LinkData();
            windowLinks.add(link);

            link.setUrl(UIDef.LINK_WINDOW_TABLE);
            link.setText(StatBuckets.getShowTimeStr(timeWindow));

            Map<String, String> tempParameterMap =
                    new HashMap<String, String>();
            tempParameterMap.putAll(parameterMap);
            tempParameterMap.put(UIDef.WINDOW,
                    StatBuckets.getShowTimeStr(timeWindow));
            link.setParamMap(tempParameterMap);

        }
    }

    public static Map<String, String> testTopologyPage() {
        Map<String, String> parameterMap = new HashMap<String, String>();
        parameterMap.put(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_TOPOLOGY);
        parameterMap.put(UIDef.TOPOLOGY, "SequenceTest-1-1431086898");

        return parameterMap;
    }

    public String getWindowsTitle() {
        return windowsTitle;
    }

    public List<LinkData> getWindowLinks() {
        return windowLinks;
    }

    public static void main(String[] args) {
        // testCluster();
        Map<String, String> parameterMap = testTopologyPage();
        try {
            WindowTablePage table = new WindowTablePage(parameterMap);

            System.out.println(table.getTables());
        } catch (Exception e) {
            // TODO Auto-generated catch block

        }
    }
}
