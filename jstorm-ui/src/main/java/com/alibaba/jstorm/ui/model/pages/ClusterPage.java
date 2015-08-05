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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.NimbusStat;
import backtype.storm.generated.NimbusSummary;
import backtype.storm.generated.SupervisorSummary;
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
import com.alibaba.jstorm.ui.model.PageIndex;
import com.alibaba.jstorm.ui.model.TableData;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

public class ClusterPage implements PageGenerator {
    private static final Logger LOG = LoggerFactory
            .getLogger(ClusterPage.class);

    public static final String TABLE_NAME_MASTER = "Nimbus Master";
    public static final String TABLE_NAME_SLAVES = "Nimbus Slaves";

    public TableData getNimbusMaster(NimbusSummary nimbusSummary,
            Map<String, Object> nimbusConf, Map<String, String> paramMap) {
        TableData nimbusMaster = new TableData();

        nimbusMaster.setName(TABLE_NAME_MASTER);
        List<String> headers = nimbusMaster.getHeaders();
        List<Map<String, ColumnData>> lines = nimbusMaster.getLines();

        headers.add(UIDef.HOST.toUpperCase());
        headers.add(UIDef.HEADER_UPTIME);
        headers.add(UIDef.HEADER_SUPERVISOR);
        headers.add(UIDef.HEADER_TOTAL_PORT);
        headers.add(UIDef.HEADER_USED_PORT);
        headers.add(UIDef.HEADER_FREE_PORT);
        headers.add(UIDef.HEADER_VERSION);
        headers.add(UIDef.HEADER_CONF);
        headers.add(UIDef.HEADER_LOGS);

        Map<String, ColumnData> line = new HashMap<String, ColumnData>();
        lines.add(line);

        ColumnData nimbusMasterColumn = new ColumnData();
        String ipPort = nimbusSummary.get_nimbus_master().get_host();
        String hostPort = UIUtils.getHostIp(ipPort);
        nimbusMasterColumn.addText(hostPort);
        line.put(UIDef.HOST.toUpperCase(), nimbusMasterColumn);

        ColumnData uptimeColumn = new ColumnData();
        String uptime = nimbusSummary.get_nimbus_master().get_uptime_secs();
        if (uptime == null) {
            uptimeColumn.addText(StatBuckets.prettyUptimeStr(0));
        } else {
            uptimeColumn.addText(StatBuckets.prettyUptimeStr(Integer
                    .valueOf(uptime)));
        }
        line.put(UIDef.HEADER_UPTIME, uptimeColumn);

        ColumnData supervisorColumn = new ColumnData();
        supervisorColumn.addText(String.valueOf(nimbusSummary
                .get_supervisor_num()));
        line.put(UIDef.HEADER_SUPERVISOR, supervisorColumn);

        ColumnData totalPortColumn = new ColumnData();
        totalPortColumn.addText(String.valueOf(nimbusSummary
                .get_total_port_num()));
        line.put(UIDef.HEADER_TOTAL_PORT, totalPortColumn);

        ColumnData usedPortColumn = new ColumnData();
        usedPortColumn
                .addText(String.valueOf(nimbusSummary.get_used_port_num()));
        line.put(UIDef.HEADER_USED_PORT, usedPortColumn);

        ColumnData freePortColumn = new ColumnData();
        freePortColumn
                .addText(String.valueOf(nimbusSummary.get_free_port_num()));
        line.put(UIDef.HEADER_FREE_PORT, freePortColumn);

        ColumnData versionColumn = new ColumnData();
        versionColumn.addText(nimbusSummary.get_version());
        line.put(UIDef.HEADER_VERSION, versionColumn);

        ColumnData confColumn = new ColumnData();
        LinkData confLink = new LinkData();
        confColumn.addLinkData(confLink);
        line.put(UIDef.HEADER_CONF, confColumn);

        confLink.setUrl(UIDef.LINK_TABLE_PAGE);
        confLink.setText(UIDef.HEADER_CONF.toLowerCase());
        confLink.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
        confLink.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_CONF);
        confLink.addParam(UIDef.CONF_TYPE, UIDef.CONF_TYPE_NIMBUS);

        ColumnData logsColumn = new ColumnData();
        LinkData logsLink = new LinkData();
        logsColumn.addLinkData(logsLink);
        line.put(UIDef.HEADER_LOGS, logsColumn);

        logsLink.setUrl(UIDef.LINK_TABLE_PAGE);
        logsLink.setText(UIDef.HEADER_LOGS.toLowerCase());
        logsLink.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
        logsLink.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_LISTLOG);
        logsLink.addParam(UIDef.HOST, UIUtils.getHost(ipPort));
        logsLink.addParam(UIDef.PORT, String.valueOf(ConfigExtension
                .getNimbusDeamonHttpserverPort(nimbusConf)));
        logsLink.addParam(UIDef.DIR, ".");

        return nimbusMaster;
    }

    public TableData getNimbusSlaves(NimbusSummary nimbusSummary,
            Map<String, Object> nimbusConf, Map<String, String> paramMap) {
        List<NimbusStat> slaves = nimbusSummary.get_nimbus_slaves();
        if (slaves == null || slaves.size() == 0) {
            return null;
        }

        TableData slavesTable = new TableData();
        List<String> headers = slavesTable.getHeaders();
        List<Map<String, ColumnData>> lines = slavesTable.getLines();
        slavesTable.setName(TABLE_NAME_SLAVES);

        headers.add(UIDef.HOST.toUpperCase());
        headers.add(UIDef.HEADER_UPTIME);
        headers.add(UIDef.HEADER_LOGS);

        for (NimbusStat slave : slaves) {

            Map<String, ColumnData> line = new HashMap<String, ColumnData>();
            lines.add(line);

            ColumnData hostColumn = new ColumnData();
            String ipPort = slave.get_host();
            String hostPort = UIUtils.getHostIp(ipPort);
            hostColumn.addText(hostPort);
            line.put(UIDef.HOST.toUpperCase(), hostColumn);

            ColumnData uptimeColumn = new ColumnData();
            String uptime = slave.get_uptime_secs();
            if (uptime == null) {
                uptimeColumn.addText(StatBuckets.prettyUptimeStr(0));
            } else {
                uptimeColumn.addText(StatBuckets.prettyUptimeStr(Integer
                        .valueOf(uptime)));
            }
            line.put(UIDef.HEADER_UPTIME, uptimeColumn);

            ColumnData logsColumn = new ColumnData();
            LinkData logsLink = new LinkData();
            logsColumn.addLinkData(logsLink);
            line.put(UIDef.HEADER_LOGS, logsColumn);

            logsLink.setUrl(UIDef.LINK_TABLE_PAGE);
            logsLink.setText(UIDef.HEADER_LOGS.toLowerCase());
            logsLink.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
            logsLink.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_LISTLOG);
            logsLink.addParam(UIDef.HOST, UIUtils.getHost(ipPort));
            logsLink.addParam(UIDef.PORT, String.valueOf(ConfigExtension
                    .getNimbusDeamonHttpserverPort(nimbusConf)));
            logsLink.addParam(UIDef.DIR, ".");
        }

        return slavesTable;

    }

    public List<TableData> getNimbus(NimbusSummary nimbusSummary,
            Map<String, Object> nimbusConf, Map<String, String> paramMap) {
        List<TableData> ret = new ArrayList<TableData>();
        TableData nimbusMaster =
                getNimbusMaster(nimbusSummary, nimbusConf, paramMap);
        ret.add(nimbusMaster);

        TableData slavesTable =
                getNimbusSlaves(nimbusSummary, nimbusConf, paramMap);
        if (slavesTable != null) {
            ret.add(slavesTable);
        }

        return ret;
    }

    public TableData getSupervisorTable(
            List<SupervisorSummary> supervisorSummaries,
            Map<String, String> paramMap) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();
        table.setName(UIDef.SUPERVISOR.toUpperCase());

        headers.add(UIDef.HOST.toUpperCase());
        headers.add(UIDef.HEADER_UPTIME);
        headers.add(UIDef.HEADER_TOTAL_PORT);
        headers.add(UIDef.HEADER_USED_PORT);

        if (supervisorSummaries == null) {
            return table;
        }
        
        long pos = JStormUtils.parseLong(paramMap.get(UIDef.POS), 0);

        for ( long index = pos; 
        		index  < supervisorSummaries.size() && index < pos + UIUtils.ONE_TABLE_PAGE_SIZE; 
        		index++ ) {
        	SupervisorSummary supervisorSummary = supervisorSummaries.get((int)index);
            Map<String, ColumnData> line = new HashMap<String, ColumnData>();
            lines.add(line);

            ColumnData hostColumn = new ColumnData();
            LinkData linkData = new LinkData();
            hostColumn.addLinkData(linkData);
            line.put(UIDef.HOST.toUpperCase(), hostColumn);

            linkData.setUrl(UIDef.LINK_TABLE_PAGE);
            String ip = supervisorSummary.get_host();
            linkData.setText(NetWorkUtils.ip2Host(ip));
            linkData.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
            linkData.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_SUPERVISOR);
            linkData.addParam(UIDef.HOST, ip);

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

        }

        return table;
    }

    @Override
    public Output generate(Map<String, String> paramMap) {
        // TODO Auto-generated method stub
        List<TableData> tables = null;

        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(paramMap);

            ClusterSummary clusterSummary = client.getClient().getClusterInfo();

            String jsonConf = client.getClient().getNimbusConf();
            Map<String, Object> nimbusConf =
                    (Map<String, Object>) Utils.from_json(jsonConf);

            NimbusSummary nimbusSummary = clusterSummary.get_nimbus();

            tables = getNimbus(nimbusSummary, nimbusConf, paramMap);

            TableData topologyTable =
                    UIUtils.getTopologyTable(clusterSummary.get_topologies(),
                            paramMap);
            tables.add(topologyTable);

            tables.add(getSupervisorTable(clusterSummary.get_supervisors(),
                    paramMap));

            Output ret = new Output();
            ret.tables = tables;
            ret.rawData = "";
            if (clusterSummary.get_supervisors().size() > UIUtils.ONE_TABLE_PAGE_SIZE) {
            	ret.pages = PageIndex.generatePageIndex(
            			clusterSummary.get_supervisors().size(), 
            			UIUtils.ONE_TABLE_PAGE_SIZE, 
            			UIDef.LINK_TABLE_PAGE, paramMap);
            }
            
            
            return ret;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            NimbusClientManager.removeClient(paramMap);
            LOG.error(e.getMessage(), e);
            

            return UIUtils.getErrorInfo(e);
        } 

    }

}
