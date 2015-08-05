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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.NettyMetric;
import backtype.storm.utils.NimbusClient;

import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.common.metric.window.StatBuckets;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.ui.NimbusClientManager;
import com.alibaba.jstorm.ui.UIDef;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.PageGenerator;
import com.alibaba.jstorm.ui.model.PageIndex;
import com.alibaba.jstorm.ui.model.TableData;
import com.alibaba.jstorm.ui.model.PageIndex.Event;
import com.alibaba.jstorm.utils.JStormUtils;

public class NettyPage implements PageGenerator {
    private static final Logger LOG = LoggerFactory
            .getLogger(NettyPage.class);

    public TableData getNettyMetricTable(NettyMetric nettyMetric,
            Map<String, String> paramMap) {
        String windowStr = paramMap.get(UIDef.WINDOW);
        Integer window = StatBuckets.getTimeKey(windowStr);
        
        if (nettyMetric == null || nettyMetric.get_connections() == null) {
            throw new RuntimeException("No netty metrics");
        }


        TreeMap<String, MetricInfo> metrics = new TreeMap<String, MetricInfo>();
        metrics.putAll(nettyMetric.get_connections());
        return UIUtils.getNettyMetricsTable(metrics, window, paramMap);
    }
    
    public List<PageIndex> getPageIndex(NettyMetric nettyMetric, Map<String, String> paramMap) {
        if (nettyMetric.get_connectionNum() == nettyMetric.get_connections().size()) {
            return new ArrayList<PageIndex>();
        }
        
        PageIndex.Event event = new PageIndex.Event();
        event.totalSize = nettyMetric.get_connectionNum();
        event.pos = JStormUtils.parseLong(paramMap.get(UIDef.POS), 0);
        event.pageSize = MetricDef.NETTY_METRICS_PACKAGE_SIZE;
        event.url = UIDef.LINK_WINDOW_TABLE;
        event.paramMap = paramMap;
        
        return PageIndex.generatePageIndex(event);
    }

    public Output generate(Map<String, String> paramMap) {
        // TODO Auto-generated method stub
        List<TableData> tables = new ArrayList<TableData>();

        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(paramMap);

            String topologyId = paramMap.get(UIDef.TOPOLOGY);
            if (topologyId == null) {
                throw new IllegalArgumentException("Not set topologyId");
            }
            
            

            NettyMetric nettyMetric = null;
            try {

                String topologyName = Common.getTopologyNameById(topologyId);
                
                String server = paramMap.get(UIDef.HOST);
                if (server == null) {
                    int pos = JStormUtils.parseInt(paramMap.get(UIDef.POS), 0);
                    nettyMetric = client.getClient().getNettyMetric(topologyName, pos);
                }else {
                    nettyMetric = client.getClient().getServerNettyMetric(topologyName, server);
                }
                
                
            } catch (org.apache.thrift.TException e) {
                throw new IllegalArgumentException(
                        "Failed to get topologyInfo of " + topologyId
                                + ", maybe it is dead");
            }

            
            TableData nettyMetricTable =
                    getNettyMetricTable(nettyMetric, paramMap);
            tables.add(nettyMetricTable);

            Output ret = new Output();
            ret.tables = tables;
            ret.rawData = "";
            ret.pages = getPageIndex(nettyMetric, paramMap);
            return ret;
        } catch (Exception e) {
            NimbusClientManager.removeClient(paramMap);
            LOG.error(e.getMessage(), e);

            return UIUtils.getErrorInfo(e);
        } 
    }

}
