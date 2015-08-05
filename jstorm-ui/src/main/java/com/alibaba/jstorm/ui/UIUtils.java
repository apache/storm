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
package com.alibaba.jstorm.ui;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.generated.ComponentSummary;
import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricWindow;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.MetricRegistry;
import com.alibaba.jstorm.common.metric.window.StatBuckets;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.ui.model.ColumnData;
import com.alibaba.jstorm.ui.model.LinkData;
import com.alibaba.jstorm.ui.model.PageGenerator;
import com.alibaba.jstorm.ui.model.PageIndex;
import com.alibaba.jstorm.ui.model.PageGenerator.Output;
import com.alibaba.jstorm.ui.model.TableData;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

public class UIUtils {
    public static ScheduledExecutorService scheduExec = Executors.newScheduledThreadPool(4); 

    public static Long ONE_TABLE_PAGE_SIZE = null;
    
    public static Map readUiConfig() {
        Map ret = Utils.readStormConfig();
        String curDir = System.getProperty("user.home");
        String confPath =
                curDir + File.separator + ".jstorm" + File.separator
                        + "storm.yaml";
        File file = new File(confPath);
        if (file.exists()) {

            FileInputStream fileStream;
            try {
                fileStream = new FileInputStream(file);
                Yaml yaml = new Yaml();

                Map clientConf = (Map) yaml.load(fileStream);

                if (clientConf != null) {
                    ret.putAll(clientConf);
                }
                
                if (ONE_TABLE_PAGE_SIZE == null) {
                	ONE_TABLE_PAGE_SIZE = ConfigExtension.getUiOneTablePageSize(clientConf);
                }
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
            }

        }
        if (ret.containsKey(Config.NIMBUS_HOST) == false) {
            ret.put(Config.NIMBUS_HOST, "localhost");

        }
        return ret;
    }

    public static String getHostIp(String ipPort) {

        if (ipPort.contains(":")) {
            String firstPart = ipPort.substring(0, ipPort.indexOf(":"));
            String lastPart = ipPort.substring(ipPort.indexOf(":"));

            return NetWorkUtils.ip2Host(firstPart) + lastPart;
        } else {
            return NetWorkUtils.ip2Host(ipPort);
        }

    }

    public static String getHost(String ipPort) {
        if (ipPort.contains(":")) {
            String firstPart = ipPort.substring(0, ipPort.indexOf(":"));

            return NetWorkUtils.ip2Host(firstPart);
        } else {
            return NetWorkUtils.ip2Host(ipPort);
        }
    }

    public static Set<String> getKeys(Collection<MetricInfo> metrics) {
        Set<String> ret = new TreeSet<String>();
        for (MetricInfo metric : metrics) {
            Set<String> oneKeys = metric.get_baseMetric().keySet();
            ret.addAll(oneKeys);
        }

        return ret;
    }

    public static String getValue(MetricInfo metric, String key, Integer window) {
        MetricWindow metricWindow = metric.get_baseMetric().get(key);
        if (metricWindow == null) {
            return String.valueOf("000.000");
        }

        Double value = metricWindow.get_metricWindow().get(window);
        if (value == null) {
            return String.valueOf("000.000");
        }

        return JStormUtils.formatSimpleDouble(value);
    }

    public static void checkKey(List<String> ret, Set<String> temp, String key) {
        if (temp.contains(key)) {
            ret.add(key);
            temp.remove(key);
        }
        return;
    }

    public static List<String> getSortedKeys(Set<String> keys) {
        List<String> ret = new ArrayList<String>();

        Set<String> temp = new TreeSet<String>();
        temp.addAll(keys);

        checkKey(ret, temp, MetricDef.CPU_USED_RATIO);
        checkKey(ret, temp, MetricDef.MEMORY_USED);
        checkKey(ret, temp, MetricDef.SEND_TPS);
        checkKey(ret, temp, MetricDef.RECV_TPS);

        checkKey(ret, temp, MetricDef.EMMITTED_NUM);
        checkKey(ret, temp, MetricDef.ACKED_NUM);
        checkKey(ret, temp, MetricDef.FAILED_NUM);
        checkKey(ret, temp, MetricDef.PROCESS_LATENCY);

        ret.addAll(temp);
        return ret;
    }

    public static TableData getMetricTable(MetricInfo metricInfo, Integer window) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();
        // table.setName(UIDef.TOPOLOGY.toUpperCase());

        Map<String, MetricWindow> baseMetric = metricInfo.get_baseMetric();
        List<String> keys = getSortedKeys(baseMetric.keySet());

        headers.addAll(keys);
        Map<String, ColumnData> line =
                getMetricLine(metricInfo, headers, window);
        lines.add(line);

        return table;
    }

    public static Map<String, ColumnData> getMetricLine(MetricInfo metricInfo,
            List<String> headers, Integer window) {
        Map<String, ColumnData> line = new HashMap<String, ColumnData>();

        for (String key : headers) {
            String value = getValue(metricInfo, key, window);

            ColumnData columnData = new ColumnData();
            columnData.addText(value);

            line.put(key, columnData);
        }

        return line;
    }

    public static void complementingTable(TableData table) {
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();

        for (Map<String, ColumnData> line : lines) {
            for (String header : headers) {
                ColumnData item = line.get(header);
                if (item == null) {
                    item = new ColumnData();
                    item.addText("000.000");
                    line.put(header, item);
                }
            }
        }

        return;
    }

    public static TableData errorTable(String erro) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();

        headers.add("Error");

        Map<String, ColumnData> line = new HashMap<String, ColumnData>();
        lines.add(line);

        ColumnData columnData = new ColumnData();
        columnData.addText(erro);
        line.put("Error", columnData);

        return table;
    }

    public static PageGenerator.Output getErrorInfo(Exception e) {
        String errMsg = JStormUtils.getErrorInfo(e);
        Output ret = new Output();
        ret.tables = new ArrayList<TableData>();
        ret.rawData =
                "!!!!!!!!!!!!!!!! Occur Exception:" + e.getMessage()
                        + " !!!!!!!!!\r\n\r\nPlease refresh once again\r\n" + errMsg;

        return ret;
    }

    public static double getDoubleValue(Double value) {
        double ret = (value != null ? value.doubleValue() : 0.0);
        return ret;
    }

    public static void getClusterInfoByName(Map conf, String clusterName) {
        List<Map> uiClusters = ConfigExtension.getUiClusters(conf);
        Map cluster = ConfigExtension.getUiClusterInfo(uiClusters, clusterName);

        conf.put(Config.STORM_ZOOKEEPER_ROOT,
                ConfigExtension.getUiClusterZkRoot(cluster));
        conf.put(Config.STORM_ZOOKEEPER_SERVERS,
                ConfigExtension.getUiClusterZkServers(cluster));
        conf.put(Config.STORM_ZOOKEEPER_PORT,
                ConfigExtension.getUiClusterZkPort(cluster));
    }

    public static TableData getTopologyTable(
            List<TopologySummary> topologySummaries,
            Map<String, String> paramMap) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();
        table.setName(UIDef.TOPOLOGY.toUpperCase());

        headers.add(UIDef.HEADER_TOPOLOGY_NAME);
        headers.add(UIDef.HEADER_TOPOLOGY_ID);
        headers.add(UIDef.HEADER_STATUS);
        headers.add(UIDef.HEADER_UPTIME);
        headers.add(UIDef.HEADER_WORKER_NUM);
        headers.add(UIDef.HEADER_TASK_NUM);
        headers.add(UIDef.HEADER_CONF);
        headers.add(UIDef.HEADER_ERROR);

        if (topologySummaries == null) {
            return table;
        }

        for (TopologySummary topologySummary : topologySummaries) {
            Map<String, ColumnData> line = new HashMap<String, ColumnData>();
            lines.add(line);

            ColumnData topologyNameColumn = new ColumnData();
            LinkData linkData = new LinkData();
            topologyNameColumn.addLinkData(linkData);
            line.put(UIDef.HEADER_TOPOLOGY_NAME, topologyNameColumn);

            linkData.setUrl(UIDef.LINK_WINDOW_TABLE);
            linkData.setText(topologySummary.get_name());
            linkData.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
            linkData.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_TOPOLOGY);
            linkData.addParam(UIDef.TOPOLOGY, topologySummary.get_id());

            ColumnData topologyIdColumn = new ColumnData();
            topologyIdColumn.addText(topologySummary.get_id());
            line.put(UIDef.HEADER_TOPOLOGY_ID, topologyIdColumn);

            ColumnData statusColumn = new ColumnData();
            statusColumn.addText(topologySummary.get_status());
            line.put(UIDef.HEADER_STATUS, statusColumn);

            ColumnData uptimeColumn = new ColumnData();
            int uptime = topologySummary.get_uptime_secs();
            uptimeColumn.addText(StatBuckets.prettyUptimeStr(uptime));
            line.put(UIDef.HEADER_UPTIME, uptimeColumn);

            ColumnData workerNumColumn = new ColumnData();
            workerNumColumn.addText(String.valueOf(topologySummary
                    .get_num_workers()));
            line.put(UIDef.HEADER_WORKER_NUM, workerNumColumn);

            ColumnData taskNumColumn = new ColumnData();
            taskNumColumn.addText(String.valueOf(topologySummary
                    .get_num_tasks()));
            line.put(UIDef.HEADER_TASK_NUM, taskNumColumn);

            ColumnData confColumn = new ColumnData();
            LinkData confLink = new LinkData();
            confColumn.addLinkData(confLink);
            line.put(UIDef.HEADER_CONF, confColumn);

            confLink.setUrl(UIDef.LINK_TABLE_PAGE);
            confLink.setText(UIDef.HEADER_CONF.toLowerCase());
            confLink.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
            confLink.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_CONF);
            confLink.addParam(UIDef.CONF_TYPE, UIDef.CONF_TYPE_TOPOLOGY);
            confLink.addParam(UIDef.TOPOLOGY, topologySummary.get_id());

            ColumnData errorColumn = new ColumnData();
            String errorInfo = String.valueOf(topologySummary.get_error_info());
            errorColumn.addText(errorInfo);
            line.put(UIDef.HEADER_ERROR, errorColumn);
        }

        return table;
    }

    public static TableData getComponentTable(TopologyInfo topologyInfo,
            List<ComponentSummary> componentSummaries,
            Map<String, MetricInfo> componentMetrics,
            Map<String, String> paramMap, Integer window) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();

        List<String> keys =
                UIUtils.getSortedKeys(UIUtils.getKeys(componentMetrics.values()));

        headers.add(UIDef.HEADER_COMPONENT_NAME);
        headers.add(UIDef.HEADER_TASK_NUM);
        headers.addAll(keys);
        headers.add(UIDef.HEADER_ERROR);

        for (ComponentSummary componentSummary : componentSummaries) {
            Map<String, ColumnData> line = new HashMap<String, ColumnData>();
            lines.add(line);

            String name = componentSummary.get_name();

            ColumnData nameColumnData = new ColumnData();
            LinkData linkData = new LinkData();
            nameColumnData.addLinkData(linkData);
            line.put(UIDef.HEADER_COMPONENT_NAME, nameColumnData);

            linkData.setUrl(UIDef.LINK_WINDOW_TABLE);
            linkData.setText(name);
            linkData.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
            linkData.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_COMPONENT);
            linkData.addParam(UIDef.TOPOLOGY, topologyInfo.get_topology()
                    .get_id());
            linkData.addParam(UIDef.COMPONENT, name);

            ColumnData taskNumColumn = new ColumnData();
            taskNumColumn.addText(String.valueOf(componentSummary
                    .get_parallel()));
            line.put(UIDef.HEADER_TASK_NUM, taskNumColumn);

            ColumnData errColumn = new ColumnData();
            line.put(UIDef.HEADER_ERROR, errColumn);
            List<ErrorInfo> errs = componentSummary.get_errors();
            if (errs == null) {
                errColumn.addText("");

            } else {
                for (ErrorInfo err : errs) {
                    errColumn.addText(err.get_error() + "\r\n");
                }

            }

            MetricInfo metric = componentMetrics.get(name);
            if (metric == null) {
                continue;
            }

            for (String key : keys) {
                String value = UIUtils.getValue(metric, key, window);
                ColumnData columnData = new ColumnData();
                columnData.addText(value);
                line.put(key, columnData);

            }

        }

        return table;
    }
    
    public static ColumnData getConnectionColumnData(Map<String, String> paramMap, String connectionName) {
        ColumnData columnData = new ColumnData();
        LinkData linkData = new LinkData();
        columnData.addLinkData(linkData);
        
        linkData.setUrl(UIDef.LINK_WINDOW_TABLE);
        linkData.setText(connectionName);
        Map<String, String> linkDataParam = new HashMap<String, String>();
        linkData.setParamMap(linkDataParam);
        linkDataParam.putAll(paramMap);
        linkDataParam.remove(UIDef.POS);
        
        
        try {
            int pos = connectionName.indexOf(":");
            if (pos > 0) {
                String source = connectionName.substring(0, pos);
                linkDataParam.put(UIDef.HOST, source);
            }
            
        } catch (Exception e) {
        }
        
        return columnData;
    }
    
    public static TableData getNettyMetricsTable(Map<String, MetricInfo> metrics, 
                    Integer window, Map<String, String> paramMap) {
        
        TableData nettyTable = new TableData();
        nettyTable.setName(MetricDef.NETTY + " Metrics " + StatBuckets.getShowTimeStr(window));
        
        List<String> nettyHeaders = nettyTable.getHeaders();
        nettyHeaders.add(UIDef.HEADER_NETWORKER_CONNECTION);
        nettyHeaders.addAll(getKeys(metrics.values()));
        
        List<Map<String, ColumnData>> nettyLines = nettyTable.getLines();
        for (Entry<String, MetricInfo> entry : metrics.entrySet()) {
            String connectionName = entry.getKey();
            MetricInfo metricInfo = entry.getValue();
            
            Map<String, ColumnData> line = getMetricLine(metricInfo, nettyHeaders, window);
            
            ColumnData columnData = getConnectionColumnData(paramMap, connectionName);
            line.put(UIDef.HEADER_NETWORKER_CONNECTION, columnData);
            
            nettyLines.add(line);
        }
        
        return nettyTable;
    }
    
    public static List<TableData> getWorkerMetricsTable(Map<String, MetricInfo> metrics,
                    Integer window, Map<String, String> paramMap) {
        List<TableData> ret = new ArrayList<TableData>();
        TableData table = new TableData();
        ret.add(table);
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();
        table.setName("Worker " + UIDef.METRICS);
        
        List<String> keys = getSortedKeys(UIUtils.getKeys(metrics.values()));
        headers.add(UIDef.PORT);
        headers.add(MetricDef.NETTY);
        headers.addAll(keys);
        
        TreeMap<String, MetricInfo> tmpMap = new TreeMap<String, MetricInfo>();
        tmpMap.putAll(metrics);
        Map<String, MetricInfo> showMap = new TreeMap<String, MetricInfo>();
        
        long pos = JStormUtils.parseLong(paramMap.get(UIDef.POS), 0);
        long index = 0;
        for (Entry<String, MetricInfo> entry : tmpMap.entrySet()) {
        	if (index < pos) {
        		index ++;
        	}else if (pos <= index && index < pos + UIUtils.ONE_TABLE_PAGE_SIZE) {
        		showMap.put(entry.getKey(), entry.getValue());
        		index++;
        	}else {
        		break;
        	}
        }
        
        for (Entry<String, MetricInfo> entry : showMap.entrySet()) {
            Map<String, ColumnData> line = new HashMap<String, ColumnData>();
            lines.add(line);
            
            String slot = entry.getKey();
            MetricInfo metric = entry.getValue();
            
            ColumnData slotColumn = new ColumnData();
            slotColumn.addText(slot);
            line.put(UIDef.PORT, slotColumn);
            
            ColumnData nettyColumn = new ColumnData();
            line.put(MetricDef.NETTY, nettyColumn);
            
            if (StringUtils.isBlank(paramMap.get(UIDef.TOPOLOGY) ) ) {
                nettyColumn.addText(MetricDef.NETTY);
            }else {
                LinkData linkData = new LinkData();
                nettyColumn.addLinkData(linkData);
                
                linkData.setUrl(UIDef.LINK_WINDOW_TABLE);
                linkData.setText(MetricDef.NETTY);
                linkData.addParam(UIDef.CLUSTER, paramMap.get(UIDef.CLUSTER));
                linkData.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_NETTY);
                linkData.addParam(UIDef.TOPOLOGY, paramMap.get(UIDef.TOPOLOGY));
            }
            
            
            for (String key : keys) {
                String value = UIUtils.getValue(metric, key, window);
                ColumnData valueColumn = new ColumnData();
                valueColumn.addText(value);
                line.put(key, valueColumn);
            }
        }
        
        return ret;
    }
    
    

    public static void main(String[] args) {
    }
}
