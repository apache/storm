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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.SupervisorWorkers;
import backtype.storm.generated.TaskComponent;
import backtype.storm.generated.WorkerSummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.window.StatBuckets;
import com.alibaba.jstorm.ui.NimbusClientManager;
import com.alibaba.jstorm.ui.UIDef;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ColumnData;
import com.alibaba.jstorm.ui.model.LinkData;
import com.alibaba.jstorm.ui.model.PageGenerator;
import com.alibaba.jstorm.ui.model.TableData;
import com.alibaba.jstorm.utils.NetWorkUtils;

public class SupervisorPage implements PageGenerator {
    private static final Logger LOG = LoggerFactory
            .getLogger(SupervisorPage.class);

    public TableData getSupervisorTable(SupervisorSummary supervisorSummary,
            Map<String, String> paramMap, Map<String, Object> nimbusConf) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();
        table.setName(UIDef.SUPERVISOR.toUpperCase());

        headers.add(UIDef.HOST.toUpperCase());
        headers.add(UIDef.IP);
        headers.add(UIDef.HEADER_UPTIME);
        headers.add(UIDef.HEADER_TOTAL_PORT);
        headers.add(UIDef.HEADER_USED_PORT);
        headers.add(UIDef.HEADER_CONF);
        headers.add(UIDef.HEADER_LOGS);

        Map<String, ColumnData> line = new HashMap<String, ColumnData>();
        lines.add(line);

        ColumnData hostColumn = new ColumnData();
        hostColumn.addText(NetWorkUtils.ip2Host(supervisorSummary.get_host()));
        line.put(UIDef.HOST.toUpperCase(), hostColumn);

        ColumnData ipColumn = new ColumnData();
        ipColumn.addText(NetWorkUtils.host2Ip(supervisorSummary.get_host()));
        line.put(UIDef.IP, ipColumn);

        ColumnData uptimeColumn = new ColumnData();
        int uptime = supervisorSummary.get_uptime_secs();
        uptimeColumn.addText(StatBuckets.prettyUptimeStr(uptime));
        line.put(UIDef.HEADER_UPTIME, uptimeColumn);

        ColumnData totalPortColumn = new ColumnData();
        totalPortColumn.addText(String.valueOf(supervisorSummary
                .get_num_workers()));
        line.put(UIDef.HEADER_TOTAL_PORT, totalPortColumn);

        ColumnData usedPortColumn = new ColumnData();
        usedPortColumn.addText(String.valueOf(supervisorSummary
                .get_num_used_workers()));
        line.put(UIDef.HEADER_USED_PORT, usedPortColumn);

        ColumnData confColumn = new ColumnData();
        LinkData confLink = new LinkData();
        confColumn.addLinkData(confLink);
        line.put(UIDef.HEADER_CONF, confColumn);

        confLink.setUrl(UIDef.LINK_TABLE_PAGE);
        confLink.setText(UIDef.HEADER_CONF.toLowerCase());
        confLink.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
        confLink.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_CONF);
        confLink.addParam(UIDef.CONF_TYPE, UIDef.CONF_TYPE_SUPERVISOR);
        confLink.addParam(UIDef.HOST,
                NetWorkUtils.host2Ip(supervisorSummary.get_host()));
        confLink.addParam(UIDef.PORT, String.valueOf(ConfigExtension
                .getSupervisorDeamonHttpserverPort(nimbusConf)));

        ColumnData logColumn = new ColumnData();
        LinkData logLink = new LinkData();
        logColumn.addLinkData(logLink);
        line.put(UIDef.HEADER_LOGS, logColumn);

        logLink.setUrl(UIDef.LINK_TABLE_PAGE);
        logLink.setText(UIDef.HEADER_LOGS.toLowerCase());
        logLink.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_LISTLOG);
        logLink.addParam(UIDef.HOST,
                NetWorkUtils.host2Ip(supervisorSummary.get_host()));
        logLink.addParam(UIDef.PORT, String.valueOf(ConfigExtension
                .getSupervisorDeamonHttpserverPort(nimbusConf)));
        logLink.addParam(UIDef.DIR, ".");

        return table;
    }

    public TableData getWokerTable(SupervisorWorkers supervisorWorkers,
            Map<String, String> paramMap, Map<String, Object> nimbusConf) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();
        table.setName(StringUtils.capitalize(UIDef.WOKER));

        headers.add(StringUtils.capitalize(UIDef.PORT));
        headers.add(UIDef.HEADER_UPTIME);
        headers.add(StringUtils.capitalize(UIDef.TOPOLOGY));
        headers.add(UIDef.HEADER_TASK_LIST);
        headers.add(UIDef.HEADER_LOG);
        headers.add(StringUtils.capitalize(UIDef.JSTACK));

        List<WorkerSummary> workerSummaries = supervisorWorkers.get_workers();
        if (workerSummaries == null) {
            LOG.error("Failed to get workers of " + paramMap.get(UIDef.HOST));
            return table;
        }

        int logServerPort =
                ConfigExtension.getSupervisorDeamonHttpserverPort(nimbusConf);

        for (WorkerSummary workerSummary : workerSummaries) {
            Map<String, ColumnData> line = new HashMap<String, ColumnData>();
            lines.add(line);

            ColumnData portColumn = new ColumnData();
            portColumn.addText(String.valueOf(workerSummary.get_port()));
            line.put(StringUtils.capitalize(UIDef.PORT), portColumn);

            ColumnData uptimeColumn = new ColumnData();
            int uptime = workerSummary.get_uptime();
            uptimeColumn.addText(StatBuckets.prettyUptimeStr(uptime));
            line.put(UIDef.HEADER_UPTIME, uptimeColumn);

            ColumnData topologyColumn = new ColumnData();
            topologyColumn.addText(workerSummary.get_topology());
            line.put(StringUtils.capitalize(UIDef.TOPOLOGY), topologyColumn);

            ColumnData taskIdColumn = new ColumnData();
            line.put(UIDef.HEADER_TASK_LIST, taskIdColumn);
            for (TaskComponent taskComponent : workerSummary.get_tasks()) {
                LinkData linkData = new LinkData();
                taskIdColumn.addLinkData(linkData);
                linkData.setUrl(UIDef.LINK_WINDOW_TABLE);
                linkData.setText(taskComponent.get_component() + "-"
                        + taskComponent.get_taskId());
                linkData.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
                linkData.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_COMPONENT);
                linkData.addParam(UIDef.TOPOLOGY, workerSummary.get_topology());
                linkData.addParam(UIDef.COMPONENT,
                        taskComponent.get_component());
            }

            ColumnData logColumn = new ColumnData();
            LinkData logLink = new LinkData();
            logColumn.addLinkData(logLink);
            line.put(UIDef.HEADER_LOG, logColumn);

            logLink.setUrl(UIDef.LINK_LOG);
            logLink.setText(UIDef.HEADER_LOG.toLowerCase());
            logLink.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
            logLink.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_LOG);
            logLink.addParam(UIDef.HOST, NetWorkUtils.host2Ip(supervisorWorkers
                    .get_supervisor().get_host()));
            logLink.addParam(UIDef.TOPOLOGY, workerSummary.get_topology());
            logLink.addParam(UIDef.PORT,
                    String.valueOf(workerSummary.get_port()));
            logLink.addParam(UIDef.LOG_SERVER_PORT,
                    String.valueOf(logServerPort));

            ColumnData jstackColumn = new ColumnData();
            LinkData jstackLink = new LinkData();
            jstackColumn.addLinkData(jstackLink);
            line.put(StringUtils.capitalize(UIDef.JSTACK), jstackColumn);

            jstackLink.setUrl(UIDef.LINK_TABLE_PAGE);
            jstackLink.setText(UIDef.JSTACK);
            jstackLink.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
            jstackLink.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_JSTACK);
            jstackLink.addParam(UIDef.HOST, supervisorWorkers.get_supervisor()
                    .get_host());
            jstackLink.addParam(UIDef.TOPOLOGY, workerSummary.get_topology());
            jstackLink.addParam(UIDef.PORT,
                    String.valueOf(workerSummary.get_port()));
            jstackLink.addParam(UIDef.LOG_SERVER_PORT,
                    String.valueOf(logServerPort));
        }
        return table;
    }

    public List<TableData> getMetricsTable(SupervisorWorkers supervisorWorkers,
            Map<String, String> paramMap, Map<String, Object> nimbusConf) {

        Map<String, MetricInfo> metrics = supervisorWorkers.get_workerMetric();
        if (metrics == null) {
            LOG.error("No metrics of "
                    + supervisorWorkers.get_supervisor().get_host());
            return null;
        }

        return UIUtils.getWorkerMetricsTable(metrics,
                StatBuckets.ALL_TIME_WINDOW, paramMap);
    }

    @Override
    public Output generate(Map<String, String> paramMap) {
        // TODO Auto-generated method stub
        List<TableData> tables = new ArrayList<TableData>();

        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(paramMap);

            String jsonConf = client.getClient().getNimbusConf();
            Map<String, Object> nimbusConf =
                    (Map<String, Object>) Utils.from_json(jsonConf);

            String host = paramMap.get(UIDef.HOST);
            if (StringUtils.isBlank(host)) {
                throw new IllegalArgumentException("Invalid parameter of host ");
            }

            SupervisorWorkers supervisorWorkers =
                    client.getClient().getSupervisorWorkers(host);

            TableData supervisorTable =
                    getSupervisorTable(supervisorWorkers.get_supervisor(),
                            paramMap, nimbusConf);
            tables.add(supervisorTable);

            TableData workerTable =
                    getWokerTable(supervisorWorkers, paramMap, nimbusConf);
            tables.add(workerTable);

            List<TableData> metricsTables =
                    getMetricsTable(supervisorWorkers, paramMap, nimbusConf);
            if (metricsTables != null) {
                tables.addAll(metricsTables);
            }

            Output ret = new Output();
            ret.tables = tables;
            ret.rawData = "";
            return ret;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            NimbusClientManager.removeClient(paramMap);
            LOG.error(e.getMessage(), e);

            return UIUtils.getErrorInfo(e);
        } 

    }

}
