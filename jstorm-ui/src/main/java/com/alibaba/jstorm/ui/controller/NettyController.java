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
package com.alibaba.jstorm.ui.controller;

import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.utils.NimbusClient;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.ui.model.UINettyMetric;
import com.alibaba.jstorm.ui.utils.NimbusClientManager;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class NettyController {
    private static final Logger LOG = LoggerFactory.getLogger(NettyController.class);
    private static final int PAGE_SIZE = MetricUtils.NETTY_METRIC_PAGE_SIZE;

    @RequestMapping(value = "/netty", method = RequestMethod.GET)
    public String show(@RequestParam(value = "cluster", required = true) String clusterName,
                       @RequestParam(value = "topology", required = true) String topology_id,
                       @RequestParam(value = "host", required = true) String host,
                       @RequestParam(value = "page", required = false) String page,
                       @RequestParam(value = "win", required = false) String win,
                       ModelMap model) {
        long start = System.currentTimeMillis();
        host = NetWorkUtils.host2Ip(host);
        int window = UIUtils.parseWindow(win);
        UIUtils.addWindowAttribute(model, window);
        int _page = JStormUtils.parseInt(page, 1);
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);

            //get netty metrics, page is from 1
            MetricInfo nettyMetric = client.getClient().getPagingNettyMetrics(topology_id, host, _page);
//            System.out.println("nettyMetric:"+nettyMetric);
            List<UINettyMetric> nettyData = getNettyData(nettyMetric, host, window);
            model.addAttribute("nettyMetrics", nettyData);
            model.addAttribute("nettyHead", UIMetricUtils.sortHead(nettyData, UINettyMetric.HEAD));

            int size = client.getClient().getNettyMetricSizeByHost(topology_id, host);
//            System.out.println("netty size:"+size);
            int pageSize = (size + PAGE_SIZE - 1) / PAGE_SIZE;
            model.addAttribute("pageSize", pageSize);
            model.addAttribute("curPage", _page);

            if (size == 0){ //check whether netty metric is turn off
                Map conf = UIUtils.getTopologyConf(clusterName, topology_id);
                boolean enable = MetricUtils.isEnableNettyMetrics(conf);
                if(!enable){
                    model.addAttribute("flush", "the netty metrics is not enabled");
                }
            }


        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            LOG.error(e.getMessage(), e);
            UIUtils.addErrorAttribute(model, e);
        }
        // page information
        model.addAttribute("clusterName", clusterName);
        model.addAttribute("host", host);
        model.addAttribute("page", "netty");
        model.addAttribute("topologyId", topology_id);
        model.addAttribute("supervisorPort", UIUtils.getSupervisorPort(clusterName));
        UIUtils.addTitleAttribute(model, "Netty Summary");

        LOG.info("netty page show cost:{}ms", System.currentTimeMillis() - start);
        return "netty";
    }

    private List<UINettyMetric> getNettyData(MetricInfo nettyMetrics, String host, int window) {
        HashMap<String, UINettyMetric> nettyData = new HashMap<>();
        if (nettyMetrics == null || nettyMetrics.get_metrics_size() == 0) {
            return new ArrayList<>(nettyData.values());
        }
        for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : nettyMetrics.get_metrics().entrySet()) {
            String name = metric.getKey();
            String[] split_name = name.split("@");


            String metricName = UIMetricUtils.extractMetricName(split_name);
            String connection = null;
            if (metricName != null) {
                connection = metricName.substring(metricName.indexOf(".") + 1);
                metricName = metricName.substring(0, metricName.indexOf("."));
            }
            MetricSnapshot snapshot = metric.getValue().get(window);

            UINettyMetric netty;
            if (nettyData.containsKey(connection)) {
                netty = nettyData.get(connection);
            } else {
                netty = new UINettyMetric(host, connection);
                nettyData.put(connection, netty);
            }
            netty.setMetricValue(snapshot, metricName);
        }
        return new ArrayList<>(nettyData.values());
    }
}
