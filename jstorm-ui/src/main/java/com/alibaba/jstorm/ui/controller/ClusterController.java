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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.generated.*;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.ui.model.*;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.alibaba.jstorm.ui.utils.NimbusClientManager;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

import backtype.storm.utils.NimbusClient;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class ClusterController {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterController.class);

    @RequestMapping(value = "/cluster", method = RequestMethod.GET)
    public String show(@RequestParam(value = "name", required = true) String name,
                       ModelMap model) {
        long start = System.currentTimeMillis();
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(name);
            ClusterSummary clusterSummary = client.getClient().getClusterInfo();

            model.addAttribute("nimbus", getNimbusEntities(clusterSummary));
            model.addAttribute("topologies", JStormUtils.thriftToMap(clusterSummary.get_topologies()));
            model.addAttribute("supervisors", JStormUtils.thriftToMap(clusterSummary.get_supervisors()));

            ClusterConfig config = UIUtils.clusterConfig.get(name);
            model.addAttribute("zkServers", getZooKeeperEntities(config));

            List<MetricInfo> clusterMetrics = client.getClient().getMetrics(JStormMetrics.CLUSTER_METRIC_KEY,
                    MetaType.TOPOLOGY.getT());
            UISummaryMetric clusterData = getClusterData(clusterMetrics, AsmWindow.M1_WINDOW);
            model.addAttribute("clusterData", clusterData);
            model.addAttribute("clusterHead", UIMetricUtils.sortHead(clusterData, UISummaryMetric.HEAD));


            //update cluster cache
            int logview_port = UIUtils.getNimbusPort(name);
            ClusterEntity ce = UIUtils.getClusterEntity(clusterSummary, name, logview_port);
            UIUtils.clustersCache.put(name, ce);
        } catch (Exception e) {
            NimbusClientManager.removeClient(name);
            LOG.error(e.getMessage(), e);
            UIUtils.addErrorAttribute(model, e);
        }

        //set nimbus port and supervisor port , if necessary
        model.addAttribute("nimbusPort", UIUtils.getNimbusPort(name));
        model.addAttribute("supervisorPort", UIUtils.getSupervisorPort(name));
        model.addAttribute("clusterName", name);
        model.addAttribute("page", "cluster");
        UIUtils.addTitleAttribute(model, "Cluster Summary");
        LOG.info("cluster page show cost:{}ms", System.currentTimeMillis() - start);
        return "cluster";
    }

    private List<NimbusEntity> getNimbusEntities(ClusterSummary cluster) {
        List<NimbusEntity> ret = new ArrayList<>();

        int task_num = 0;
        for (TopologySummary ts : cluster.get_topologies()) {
            task_num += ts.get_numTasks();
        }
        int topology_num = cluster.get_topologies_size();

        NimbusSummary ns = cluster.get_nimbus();
        NimbusEntity master = new NimbusEntity(ns.get_nimbusMaster().get_host(),
                ns.get_nimbusMaster().get_uptimeSecs(), ns.get_supervisorNum(), ns.get_totalPortNum(),
                ns.get_usedPortNum(), ns.get_freePortNum(), task_num, topology_num, ns.get_version());
        ret.add(master);

        for (NimbusStat s : ns.get_nimbusSlaves()) {
            NimbusEntity slave = new NimbusEntity(s.get_host(), s.get_uptimeSecs(), ns.get_version());
            ret.add(slave);
        }
        return ret;
    }


    private List<ZooKeeperEntity> getZooKeeperEntities(ClusterConfig config) {
        List<ZooKeeperEntity> ret = new ArrayList<>();
        if (config != null) {
            String zkPort = String.valueOf(config.getZkPort());
            if (config.getZkServers() != null) {
                for (String ip : config.getZkServers()) {
                    ret.add(new ZooKeeperEntity(NetWorkUtils.ip2Host(ip), NetWorkUtils.host2Ip(ip), zkPort));
                }
            }
        }
        return ret;
    }

    private UISummaryMetric getClusterData(List<MetricInfo> infos, int window) {
        if (infos == null || infos.size() == 0) {
            return null;
        }
        MetricInfo info = infos.get(infos.size() - 1);
        UISummaryMetric clusterMetric = new UISummaryMetric();
        for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
            String name = metric.getKey();
            String[] split_name = name.split("@");
            String metricName = UIMetricUtils.extractMetricName(split_name);

            if (!metric.getValue().containsKey(window)) {
                LOG.info("cluster snapshot {} missing window:{}", metric.getKey(), window);
                continue;
            }
            MetricSnapshot snapshot = metric.getValue().get(window);

            clusterMetric.setMetricValue(snapshot, metricName);
        }
        return clusterMetric;
    }

}
