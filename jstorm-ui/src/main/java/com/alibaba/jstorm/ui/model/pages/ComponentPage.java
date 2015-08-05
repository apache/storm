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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.window.StatBuckets;
import com.alibaba.jstorm.ui.NimbusClientManager;
import com.alibaba.jstorm.ui.UIDef;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ColumnData;
import com.alibaba.jstorm.ui.model.LinkData;
import com.alibaba.jstorm.ui.model.PageGenerator;
import com.alibaba.jstorm.ui.model.PageIndex;
import com.alibaba.jstorm.ui.model.TableData;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

import backtype.storm.generated.ComponentSummary;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricWindow;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

public class ComponentPage implements PageGenerator {
    private static final Logger LOG = LoggerFactory
            .getLogger(ComponentPage.class);

    public String getWindowStr(Integer window) {
        return "(" + StatBuckets.prettyUptimeStr(window) + ")";
    }

    public TableData getComponentSummary(TopologyInfo topologyInfo,
            ComponentSummary componentSummary, Map<String, String> paramMap,
            Integer window) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();
        table.setName(componentSummary.get_name() + getWindowStr(window));

        headers.add(UIDef.HEADER_COMPONENT_NAME);
        headers.add(UIDef.HEADER_TOPOLOGY_NAME);
        headers.add(UIDef.HEADER_TASK_NUM);
        headers.add(UIDef.HEADER_COMPONENT_TYPE);

        Map<String, ColumnData> line = new HashMap<String, ColumnData>();
        lines.add(line);

        ColumnData nameColumn = new ColumnData();
        nameColumn.addText(componentSummary.get_name());
        line.put(UIDef.HEADER_COMPONENT_NAME, nameColumn);

        ColumnData topologyColumn = new ColumnData();
        LinkData linkData = new LinkData();
        topologyColumn.addLinkData(linkData);
        line.put(UIDef.HEADER_TOPOLOGY_NAME, topologyColumn);

        linkData.setUrl(UIDef.LINK_WINDOW_TABLE);
        linkData.setText(topologyInfo.get_topology().get_name());
        linkData.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
        linkData.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_TOPOLOGY);
        linkData.addParam(UIDef.TOPOLOGY, topologyInfo.get_topology().get_id());
        linkData.addParam(UIDef.WINDOW, StatBuckets.prettyUptimeStr(window));

        ColumnData taskNumColumn = new ColumnData();
        taskNumColumn.addText(String.valueOf(componentSummary.get_parallel()));
        line.put(UIDef.HEADER_TASK_NUM, taskNumColumn);

        ColumnData typeColumn = new ColumnData();
        typeColumn.addText(componentSummary.get_type());
        line.put(UIDef.HEADER_COMPONENT_TYPE, typeColumn);

        return table;
    }

    public TableData getComponentMetrics(TopologyInfo topologyInfo,
            ComponentSummary componentSummary, Map<String, String> paramMap,
            Integer window) {

        MetricInfo metricInfo =
                topologyInfo.get_metrics().get_componentMetric()
                        .get(componentSummary.get_name());
        if (metricInfo == null) {
            LOG.info("No component metric  of " + componentSummary.get_name());
            return null;
        }

        TableData table = UIUtils.getMetricTable(metricInfo, window);
        table.setName(componentSummary.get_name() + "-Metrics-"
                + getWindowStr(window));
        return table;
    }

    public TableData getStreamMetrics(
            Map<String, Map<String, MetricWindow>> streamMetric, Integer window) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();

        headers.add(UIDef.HEADER_STREAM_ID);
        headers.addAll(UIUtils.getSortedKeys(streamMetric.keySet()));

        Map<String, Map<String, ColumnData>> lineMap =
                new HashMap<String, Map<String, ColumnData>>();
        for (Entry<String, Map<String, MetricWindow>> entry : streamMetric
                .entrySet()) {
            String metricName = entry.getKey();
            Map<String, MetricWindow> streamMetricWindow = entry.getValue();
            for (Entry<String, MetricWindow> streamEntry : streamMetricWindow
                    .entrySet()) {
                String streamId = streamEntry.getKey();
                MetricWindow metric = streamEntry.getValue();

                Map<String, ColumnData> line = lineMap.get(streamId);
                if (line == null) {
                    line = new HashMap<String, ColumnData>();
                    lineMap.put(streamId, line);
                }

                String value =
                        String.valueOf(metric.get_metricWindow().get(window));

                ColumnData columnData = new ColumnData();
                columnData.addText(value);
                line.put(metricName, columnData);
            }
        }

        for (Entry<String, Map<String, ColumnData>> entry : lineMap.entrySet()) {
            String streamId = entry.getKey();
            Map<String, ColumnData> line = entry.getValue();

            lines.add(line);

            ColumnData columnData = new ColumnData();
            columnData.addText(streamId);
            line.put(UIDef.HEADER_STREAM_ID, columnData);

        }

        UIUtils.complementingTable(table);

        return table;
    }

    public TableData getInputComponentMetrics(TopologyInfo topologyInfo,
            ComponentSummary componentSummary, Map<String, String> paramMap,
            Integer window) {
        String name = componentSummary.get_name();
        MetricInfo metric =
                topologyInfo.get_metrics().get_componentMetric().get(name);
        if (metric == null) {
            LOG.info("No component metric  of " + name);
            return null;
        }

        Map<String, Map<String, MetricWindow>> input = metric.get_inputMetric();
        if (input == null || input.size() == 0) {
            LOG.info("No input metric  of " + name);
            return null;
        }

        TableData table = getStreamMetrics(input, window);
        table.setName("Input stats" + getWindowStr(window));

        return table;
    }

    public TableData getOutputComponentMetrics(TopologyInfo topologyInfo,
            ComponentSummary componentSummary, Map<String, String> paramMap,
            Integer window) {
        String name = componentSummary.get_name();
        MetricInfo metric =
                topologyInfo.get_metrics().get_componentMetric().get(name);
        if (metric == null) {
            LOG.info("No component metric  of " + componentSummary.get_name());
            return null;
        }

        Map<String, Map<String, MetricWindow>> output =
                metric.get_outputMetric();
        if (output == null || output.size() == 0) {
            LOG.info("No output metric  of " + name);
            return null;
        }

        TableData table = getStreamMetrics(output, window);
        table.setName("Output stats" + getWindowStr(window));

        return table;
    }

    public TableData getTaskSummary(ComponentSummary componentSummary,
            List<TaskSummary> tasks, Map<String, String> paramMap,
            String topologyId, Map<String, Object> nimbusConf,
            List<TaskSummary> showTasks) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();
        table.setName("Tasks");

        headers.add(UIDef.HEADER_TASK_ID);
        headers.add(UIDef.HEADER_STATUS);
        headers.add(UIDef.HEADER_UPTIME);
        headers.add(StringUtils.capitalize(UIDef.HOST));
        headers.add(StringUtils.capitalize(UIDef.PORT));
        headers.add(UIDef.HEADER_LOG);
        headers.add(UIDef.HEADER_ERROR);

        List<Integer> taskIds = componentSummary.get_task_ids();
        Set<Integer> taskIdSet = new HashSet<Integer>();
        taskIdSet.addAll(taskIds);
        

        long pos = JStormUtils.parseLong(paramMap.get(UIDef.POS), 0);
        long index = 0;
        
        Collections.sort(tasks);
        for (TaskSummary task : tasks) {
        	
            Integer taskId = task.get_task_id();
            if (taskIdSet.contains(taskId) == false) {
                continue;
            }
            
            if (index < pos) {
        		index ++;
        		continue;
        	}else if (pos <= index && index < pos + UIUtils.ONE_TABLE_PAGE_SIZE) {
        		showTasks.add(task);
                taskIdSet.remove(taskId);
        		index++;
        	}else {
        		break;
        	}
            
            
        }

        int logPort =
                ConfigExtension.getSupervisorDeamonHttpserverPort(nimbusConf);
        for (TaskSummary task : showTasks) {
            Map<String, ColumnData> line = new HashMap<String, ColumnData>();
            lines.add(line);

            ColumnData taskIdColumn = new ColumnData();
            taskIdColumn.addText(String.valueOf(task.get_task_id()));
            line.put(UIDef.HEADER_TASK_ID, taskIdColumn);

            ColumnData statusColumn = new ColumnData();
            statusColumn.addText(task.get_status());
            line.put(UIDef.HEADER_STATUS, statusColumn);

            ColumnData uptimeColumn = new ColumnData();
            int uptime = task.get_uptime();
            uptimeColumn.addText(StatBuckets.prettyUptimeStr(uptime));
            line.put(UIDef.HEADER_UPTIME, uptimeColumn);

            ColumnData hostColumn = new ColumnData();
            LinkData linkData = new LinkData();
            hostColumn.addLinkData(linkData);
            line.put(StringUtils.capitalize(UIDef.HOST), hostColumn);

            linkData.setUrl(UIDef.LINK_TABLE_PAGE);
            linkData.setText(NetWorkUtils.ip2Host(task.get_host()));
            linkData.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
            linkData.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_SUPERVISOR);
            linkData.addParam(UIDef.HOST, task.get_host());

            ColumnData portColumn = new ColumnData();
            portColumn.addText(String.valueOf(task.get_port()));
            line.put(StringUtils.capitalize(UIDef.PORT), portColumn);

            ColumnData logColumn = new ColumnData();
            LinkData logLink = new LinkData();
            logColumn.addLinkData(logLink);
            line.put(UIDef.HEADER_LOG, logColumn);

            logLink.setUrl(UIDef.LINK_LOG);
            logLink.setText(UIDef.HEADER_LOG.toLowerCase());
            logLink.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
            logLink.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_LOG);
            logLink.addParam(UIDef.HOST, task.get_host());
            logLink.addParam(UIDef.TOPOLOGY, topologyId);
            logLink.addParam(UIDef.PORT, String.valueOf(task.get_port()));
            logLink.addParam(UIDef.LOG_SERVER_PORT, String.valueOf(logPort));

            ColumnData errColumn = new ColumnData();
            List<ErrorInfo> errList = task.get_errors();
            if (errList == null || errList.size() == 0) {
                errColumn.addText("");
            } else {
                for (ErrorInfo err : errList) {
                    errColumn.addText(err.get_error() + "\r\n");
                }
            }
            line.put(UIDef.HEADER_ERROR, errColumn);
        }

        return table;

    }

    public TableData getTaskMetrics(TopologyInfo topologyInfo, List<TaskSummary> showTasks,
            ComponentSummary componentSummary, Map<String, String> paramMap,
            Integer window) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();

        Map<Integer, MetricInfo> allTaskMetrics =
                topologyInfo.get_metrics().get_taskMetric();
        Map<Integer, MetricInfo> taskMetrics =
                new TreeMap<Integer, MetricInfo>();

        for ( TaskSummary taskSummary : showTasks) {
        	Integer taskId = taskSummary.get_task_id();
            MetricInfo metric = allTaskMetrics.get(taskId);
            if (metric == null) {
                LOG.error("No task metric of " + taskId);
                continue;
            }

            taskMetrics.put(taskId, metric);
        }

        headers.add(UIDef.HEADER_TASK_ID);
        List<String> keys =
                UIUtils.getSortedKeys(UIUtils.getKeys(taskMetrics.values()));
        headers.addAll(keys);

        for (Entry<Integer, MetricInfo> entry : taskMetrics.entrySet()) {
            Integer taskId = entry.getKey();
            MetricInfo metric = entry.getValue();

            Map<String, ColumnData> line =
                    UIUtils.getMetricLine(metric, headers, window);

            ColumnData taskIdColumn = new ColumnData();
            taskIdColumn.addText(String.valueOf(taskId));
            line.put(UIDef.HEADER_TASK_ID, taskIdColumn);

            lines.add(line);
        }

        return table;
    }

    public Output generate(Map<String, String> paramMap) {
        List<TableData> tables = new ArrayList<TableData>();

        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(paramMap);

            String jsonConf = client.getClient().getNimbusConf();
            Map<String, Object> nimbusConf =
                    (Map<String, Object>) Utils.from_json(jsonConf);

            String topologyId = paramMap.get(UIDef.TOPOLOGY);
            if (topologyId == null) {
                throw new IllegalArgumentException("Not set topologyId");
            }

            String componentNam = paramMap.get(UIDef.COMPONENT);
            if (componentNam == null) {
                throw new IllegalArgumentException("Not set ComponentNam");
            }

            String windowStr = paramMap.get(UIDef.WINDOW);
            Integer window = StatBuckets.getTimeKey(windowStr);

            TopologyInfo topologyInfo =
                    client.getClient().getTopologyInfo(topologyId);
            ComponentSummary componentSummary = null;
            List<ComponentSummary> componentSummaries =
                    topologyInfo.get_components();
            for (ComponentSummary item : componentSummaries) {
                if (item.get_name().equals(componentNam)) {
                    componentSummary = item;
                    break;
                }
            }

            if (componentSummary == null) {
                throw new IllegalArgumentException("No Component of "
                        + componentNam);
            }

            List<ComponentSummary> myComponentSummaryList =
                    new ArrayList<ComponentSummary>();
            myComponentSummaryList.add(componentSummary);
            TableData componentTable =
                    UIUtils.getComponentTable(topologyInfo,
                            myComponentSummaryList, topologyInfo.get_metrics()
                                    .get_componentMetric(), paramMap, window);
            tables.add(componentTable);

            TableData inputTable =
                    getInputComponentMetrics(topologyInfo, componentSummary,
                            paramMap, window);
            if (inputTable != null) {
                tables.add(inputTable);
            }

            TableData outputTable =
                    getOutputComponentMetrics(topologyInfo, componentSummary,
                            paramMap, window);
            if (outputTable != null) {
                tables.add(outputTable);
            }

            List<TaskSummary> showTasks = new ArrayList<TaskSummary>();
            TableData taskSummaryTable =
                    getTaskSummary(componentSummary, topologyInfo.get_tasks(),
                            paramMap, topologyId, nimbusConf, showTasks);
            tables.add(taskSummaryTable);

            TableData taskMetric =
                    getTaskMetrics(topologyInfo, showTasks, componentSummary, paramMap,
                            window);
            tables.add(taskMetric);

            Output ret = new Output();
            ret.tables = tables;
            ret.rawData = "";
            if (componentSummary.get_task_ids().size() > UIUtils.ONE_TABLE_PAGE_SIZE) {
            	ret.pages = PageIndex.generatePageIndex(
            			componentSummary.get_task_ids().size(), 
            			UIUtils.ONE_TABLE_PAGE_SIZE, 
            			UIDef.LINK_WINDOW_TABLE, paramMap);
            }
            return ret;
        } catch (Exception e) {
            NimbusClientManager.removeClient(paramMap);
            
            LOG.error(e.getMessage(), e);

            return UIUtils.getErrorInfo(e);
        } 
    }
}
