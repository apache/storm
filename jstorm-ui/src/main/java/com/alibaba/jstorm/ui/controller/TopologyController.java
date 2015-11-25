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

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import com.alibaba.jstorm.ui.model.*;
import com.alibaba.jstorm.ui.utils.NimbusClientManager;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.*;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class TopologyController {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyController.class);

    @RequestMapping(value = "/topology", method = RequestMethod.GET)
    public String show(@RequestParam(value = "cluster", required = true) String cluster,
                       @RequestParam(value = "id") String id,
                       @RequestParam(value = "win", required = false) String win,
                       ModelMap model) {
        long start = System.currentTimeMillis();
        LOG.info("request topology info for cluster name: " + cluster + " id:" + id);
        int window = UIUtils.parseWindow(win);
        UIUtils.addWindowAttribute(model, window);
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(cluster);
            TopologyInfo topologyInfo = client.getClient().getTopologyInfo(id);
//            System.out.println("topologyinfo worker metric:" + topologyInfo.get_metrics().get_workerMetric());
            model.addAttribute("topology", JStormUtils.thriftToMap(topologyInfo.get_topology()));
//            model.addAttribute("tasks", getTaskEntities(topologyInfo));       //remove tasks stat
            model.addAttribute("supervisorPort", UIUtils.getSupervisorPort(cluster));

            MetricInfo topologyMetrics = topologyInfo.get_metrics().get_topologyMetric();
//            List<MetricInfo> topologyMetrics = client.getClient().getMetrics(id, MetaType.TOPOLOGY.getT());
//            System.out.println("topologyMetrics:" + topologyMetrics);
            UISummaryMetric topologyData = getTopologyData(topologyMetrics, window);
            model.addAttribute("topologyData", topologyData);
            model.addAttribute("topologyHead", UIMetricUtils.sortHead(topologyData, UISummaryMetric.HEAD));

            MetricInfo componentMetrics = topologyInfo.get_metrics().get_componentMetric();
//            List<MetricInfo> componentMetrics = client.getClient().getMetrics(id, MetaType.COMPONENT.getT());
//            System.out.println("componentMetrics:" + componentMetrics);
            List<UIUserDefinedMetric> userDefinedMetrics = Lists.newArrayList();
            List<UIComponentMetric> componentData = getComponentData(componentMetrics, window,
                    topologyInfo.get_components(), userDefinedMetrics);
            model.addAttribute("componentData", componentData);
            model.addAttribute("componentHead", UIMetricUtils.sortHead(componentData, UIComponentMetric.HEAD));
            model.addAttribute("userDefinedMetrics", userDefinedMetrics);
//            System.out.println("componentHead:" + BasicMetric.sortHead(componentData));

            MetricInfo workerMetrics = topologyInfo.get_metrics().get_workerMetric();
//            List<MetricInfo> workerMetrics = client.getClient().getMetrics(id, MetaType.WORKER.getT());
//            System.out.println("workerMetrics:" + workerMetrics);
            List<UIWorkerMetric> workerData = getWorkerData(workerMetrics, id, window);
            model.addAttribute("workerData", workerData);
            model.addAttribute("workerHead", UIMetricUtils.sortHead(workerData, UIWorkerMetric.HEAD));


            List<TaskEntity> taskData = getTaskEntities(topologyInfo);
            model.addAttribute("taskData", taskData);

        } catch (NotAliveException nae) {
            model.addAttribute("flush", String.format("The topology: %s is dead.", id));
        } catch (Exception e) {
            NimbusClientManager.removeClient(cluster);
            LOG.error(e.getMessage(), e);
            UIUtils.addErrorAttribute(model, e);
        }
        model.addAttribute("page", "topology");
        model.addAttribute("clusterName", cluster);
        UIUtils.addTitleAttribute(model, "Topology Summary");

        LOG.info("topology page show cost:{}ms", System.currentTimeMillis() - start);
        return "topology";
    }

    private UISummaryMetric getTopologyData(MetricInfo info, int window) {
        UISummaryMetric topoMetric = new UISummaryMetric();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String metricName = UIMetricUtils.extractMetricName(split_name);

                if (!metric.getValue().containsKey(window)) {
                    LOG.info("topology snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);

                topoMetric.setMetricValue(snapshot, metricName);
            }
        }
        return topoMetric;
    }

    private List<UIWorkerMetric> getWorkerData(MetricInfo info, String topology, int window) {
        Map<String, UIWorkerMetric> workerData = new HashMap<>();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String host = UIMetricUtils.extractComponentName(split_name);
                String port = UIMetricUtils.extractTaskId(split_name);
                String key = host + ":" + port;
                String metricName = UIMetricUtils.extractMetricName(split_name);


                if (!metric.getValue().containsKey(window)) {
                    LOG.info("worker snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);

                UIWorkerMetric workerMetric;
                if (workerData.containsKey(key)) {
                    workerMetric = workerData.get(key);
                } else {
                    workerMetric = new UIWorkerMetric(host, port, topology);
                    workerData.put(key, workerMetric);
                }
                workerMetric.setMetricValue(snapshot, metricName);
            }
        }
        return new ArrayList<>(workerData.values());
    }

    private List<UIComponentMetric> getComponentData(MetricInfo info, int window,
                                                   List<ComponentSummary> componentSummaries,
                                                   List<UIUserDefinedMetric> userDefinedMetrics) {
        Map<String, UIComponentMetric> componentData = new HashMap<>();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String compName = UIMetricUtils.extractComponentName(split_name);
                String metricName = UIMetricUtils.extractMetricName(split_name);
                String group = UIMetricUtils.extractGroup(split_name);
                String parentComp = null;
                if (metricName != null && metricName.contains(".")) {
                    parentComp = metricName.split("\\.")[0];
                    metricName = metricName.split("\\.")[1];
                }

                if (!metric.getValue().containsKey(window)) {
                    LOG.info("component snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);

                if (group != null && group.equals("udf")){
                    UIUserDefinedMetric udm = new UIUserDefinedMetric(metricName, compName);
                    udm.setValue(UIMetricUtils.getMetricValue(snapshot));
                    udm.setType(snapshot.get_metricType());
                    userDefinedMetrics.add(udm);
                }else {
                    UIComponentMetric compMetric;
                    if (componentData.containsKey(compName)) {
                        compMetric = componentData.get(compName);
                    } else {
                        compMetric = new UIComponentMetric(compName);
                        componentData.put(compName, compMetric);
                    }
                    compMetric.setMetricValue(snapshot, parentComp, metricName);
                }
            }
        }
        //merge sub metrics
        for (UIComponentMetric comp : componentData.values()) {
            comp.mergeValue();
        }
        //combine the summary info into metrics
        TreeMap<String, UIComponentMetric> ret = new TreeMap<>();
        for (ComponentSummary summary : componentSummaries) {
            String compName = summary.get_name();
            UIComponentMetric compMetric;
            if (componentData.containsKey(compName)) {
                compMetric = componentData.get(compName);
                compMetric.setParallel(summary.get_parallel());
                compMetric.setType(summary.get_type());
                compMetric.setErrors(summary.get_errors());
            } else {
                compMetric = new UIComponentMetric(compName, summary.get_parallel(), summary.get_type());
                compMetric.setErrors(summary.get_errors());
                componentData.put(compName, compMetric);
            }
            String key = compMetric.getType() + compName;
            if (compName.startsWith("__")) {
                key = "a" + key;
            }
            compMetric.setSortedKey(key);
            ret.put(key, compMetric);
        }
        return new ArrayList<>(ret.descendingMap().values());
    }


    private List<TaskEntity> getTaskEntities(TopologyInfo topologyInfo) {
        Map<Integer, TaskEntity> tasks = new HashMap<>();
        for (TaskSummary ts : topologyInfo.get_tasks()) {
            tasks.put(ts.get_taskId(), new TaskEntity(ts));
        }
        for (ComponentSummary cs : topologyInfo.get_components()) {
            String compName = cs.get_name();
            String type = cs.get_type();
            for (int id : cs.get_taskIds()) {
                if (tasks.containsKey(id)) {
                    tasks.get(id).setComponent(compName);
                    tasks.get(id).setType(type);
                } else {
                    LOG.info("missing task id:{}", id);
                }
            }
        }
        return new ArrayList<>(tasks.values());
    }

}
