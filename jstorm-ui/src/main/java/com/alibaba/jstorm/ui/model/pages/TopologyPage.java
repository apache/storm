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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ComponentSummary;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;

import com.alibaba.jstorm.common.metric.window.StatBuckets;
import com.alibaba.jstorm.ui.NimbusClientManager;
import com.alibaba.jstorm.ui.UIDef;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.PageGenerator;
import com.alibaba.jstorm.ui.model.PageIndex;
import com.alibaba.jstorm.ui.model.TableData;

public class TopologyPage implements PageGenerator {
    private static final Logger LOG = LoggerFactory
            .getLogger(TopologyPage.class);

    public TableData getTopologyState(TopologyInfo topologyInfo,
            Map<String, String> paramMap) {

        TopologyMetric topologyMetrics = topologyInfo.get_metrics();

        String windowStr = paramMap.get(UIDef.WINDOW);
        Integer window = StatBuckets.getTimeKey(windowStr);

        TableData table =
                UIUtils.getMetricTable(topologyMetrics.get_topologyMetric(),
                        window);
        table.setName(UIDef.HEADER_TOPOLOGY_METRICS + "("
                + StatBuckets.getShowTimeStr(window) + ")");

        return table;
    }

    public List<TableData> getComponentTables(TopologyInfo topologyInfo,
            Map<String, String> paramMap) {

        List<ComponentSummary> componentSummaries =
                topologyInfo.get_components();
        Map<String, MetricInfo> componentMetric =
                topologyInfo.get_metrics().get_componentMetric();

        List<ComponentSummary> spoutComponentSummary =
                new ArrayList<ComponentSummary>();
        Map<String, MetricInfo> spoutComponentMetric =
                new HashMap<String, MetricInfo>();

        List<ComponentSummary> boltComponentSummary =
                new ArrayList<ComponentSummary>();
        Map<String, MetricInfo> boltComponentMetric =
                new HashMap<String, MetricInfo>();

        for (ComponentSummary componentSummary : componentSummaries) {
            String type = componentSummary.get_type();
            String name = componentSummary.get_name();
            if (type.equals(UIDef.BOLT)) {
                boltComponentSummary.add(componentSummary);
                MetricInfo metric = componentMetric.get(name);
                if (metric != null) {
                    boltComponentMetric.put(name, metric);
                } else {
                    LOG.warn("No component metric of " + name);
                }
            } else if (type.equals(UIDef.SPOUT)) {
                spoutComponentSummary.add(componentSummary);
                MetricInfo metric = componentMetric.get(name);
                if (metric != null) {
                    spoutComponentMetric.put(name, metric);
                } else {
                    LOG.warn("No component metric of " + name);
                }
            } else {
                LOG.warn("No component type of " + name + ":" + type);
            }
        }

        String windowStr = paramMap.get(UIDef.WINDOW);
        Integer window = StatBuckets.getTimeKey(windowStr);

        List<TableData> ret = new ArrayList<TableData>();
        TableData spoutTable =
                UIUtils.getComponentTable(topologyInfo, spoutComponentSummary,
                        spoutComponentMetric, paramMap, window);
        spoutTable.setName(UIDef.SPOUT + "-"
                + StatBuckets.getShowTimeStr(window));
        ret.add(spoutTable);

        TableData boltTable =
                UIUtils.getComponentTable(topologyInfo, boltComponentSummary,
                        boltComponentMetric, paramMap, window);
        boltTable
                .setName(UIDef.BOLT + "-" + StatBuckets.getShowTimeStr(window));
        ret.add(boltTable);

        return ret;
    }

    public List<TableData> getWorkerMetricTable(TopologyInfo topologyInfo,
            Map<String, String> paramMap) {
        String windowStr = paramMap.get(UIDef.WINDOW);
        Integer window = StatBuckets.getTimeKey(windowStr);
        
        if (topologyInfo.get_metrics() == null || topologyInfo.get_metrics()
                .get_workerMetric() == null) {
            return null;
        }

        return UIUtils.getWorkerMetricsTable(topologyInfo.get_metrics()
                .get_workerMetric(), window, paramMap);
    }

    public Output generate(Map<String, String> paramMap) {
        // TODO Auto-generated method stub
        LOG.info("Begin TopologyPage " + new Date());
        
        
        List<TableData> tables = new ArrayList<TableData>();

        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(paramMap);

            String topologyId = paramMap.get(UIDef.TOPOLOGY);
            if (topologyId == null) {
                throw new IllegalArgumentException("Not set topologyId");
            }

            TopologyInfo topologyInfo = null;
            try {

                topologyInfo = client.getClient().getTopologyInfo(topologyId);
            } catch (org.apache.thrift.TException e) {
                throw new IllegalArgumentException(
                        "Failed to get topologyInfo of " + topologyId
                                + ", maybe it is dead");
            }

            List<TopologySummary> topologyList =
                    new ArrayList<TopologySummary>();
            topologyList.add(topologyInfo.get_topology());
            TableData topologyTable =
                    UIUtils.getTopologyTable(topologyList, paramMap);
            tables.add(topologyTable);

            TableData topologyMetricTable =
                    getTopologyState(topologyInfo, paramMap);
            tables.add(topologyMetricTable);

            List<TableData> componentTables =
                    getComponentTables(topologyInfo, paramMap);
            tables.addAll(componentTables);

            List<TableData> workerMetricTables =
                    getWorkerMetricTable(topologyInfo, paramMap);
            if (workerMetricTables != null) {
                tables.addAll(workerMetricTables);
            }

            Output ret = new Output();
            ret.tables = tables;
            ret.rawData = "";
            if (topologyInfo.get_metrics().get_workerMetric().size() >
            		UIUtils.ONE_TABLE_PAGE_SIZE) {
            	ret.pages = PageIndex.generatePageIndex(
            			topologyInfo.get_metrics().get_workerMetric().size(), 
            			UIUtils.ONE_TABLE_PAGE_SIZE, 
            			UIDef.LINK_WINDOW_TABLE, paramMap);
            }
            
            LOG.info("Finish TopologyPage " + new Date());
            return ret;
        } catch (Exception e) {
            NimbusClientManager.removeClient(paramMap);
            
            LOG.error(e.getMessage(), e);

            return UIUtils.getErrorInfo(e);
        } 
    }

}
