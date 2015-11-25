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
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.ui.model.UIComponentMetric;
import com.alibaba.jstorm.ui.model.TaskEntity;
import com.alibaba.jstorm.ui.model.UITaskMetric;
import com.alibaba.jstorm.ui.utils.NimbusClientManager;
import com.alibaba.jstorm.ui.utils.UIMetricUtils;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;
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
public class ComponentController {
    private static final Logger LOG = LoggerFactory.getLogger(ComponentController.class);

    @RequestMapping(value = "/component", method = RequestMethod.GET)
    public String show(@RequestParam(value = "cluster", required = true) String clusterName,
                       @RequestParam(value = "topology", required = true) String topology_id,
                       @RequestParam(value = "component", required = true) String component,
                       @RequestParam(value = "win", required = false) String win,
                       ModelMap model) {
        long start = System.currentTimeMillis();
        int window = UIUtils.parseWindow(win);
        UIUtils.addWindowAttribute(model, window);
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);

            //get Component Metric
            TopologyInfo topologyInfo = client.getClient().getTopologyInfo(topology_id);
            MetricInfo componentMetrics = topologyInfo.get_metrics().get_componentMetric();
//            List<MetricInfo> componentMetrics = client.getClient().getMetrics(topology_id, MetaType.COMPONENT.getT());
            UIComponentMetric componentMetric = getComponentMetric(componentMetrics, window,
                    component, topologyInfo.get_components());
            model.addAttribute("comp", componentMetric);
            model.addAttribute("compHead", UIMetricUtils.sortHead(componentMetric, UIComponentMetric.HEAD));

            //get Task Stat
            model.addAttribute("tasks", getTaskEntities(topologyInfo, component));

            //get Task metrics
//            MetricInfo taskMetrics = topologyInfo.get_metrics().get_taskMetric();
            MetricInfo taskMetrics = client.getClient().getTaskMetrics(topology_id, component);
//            System.out.println("taskMetrics:" + taskMetrics);
            List<UITaskMetric> taskData = getTaskData(taskMetrics, component, window);
            model.addAttribute("taskData", taskData);
            model.addAttribute("taskHead", UIMetricUtils.sortHead(taskData, UITaskMetric.HEAD));

        } catch (NotAliveException nae) {
            model.addAttribute("flush", String.format("The topology: %s is dead.", topology_id));
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            LOG.error(e.getMessage(), e);
            UIUtils.addErrorAttribute(model, e);
        }

        // page information
        model.addAttribute("clusterName", clusterName);
        model.addAttribute("topologyId", topology_id);
        model.addAttribute("compName", component);
        model.addAttribute("page", "component");
        model.addAttribute("supervisorPort", UIUtils.getSupervisorPort(clusterName));
        UIUtils.addTitleAttribute(model, "Component Summary");
        try {
            String topologyName = Common.topologyIdToName(topology_id);
            model.addAttribute("topologyName", topologyName);
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }

        LOG.info("component page show cost:{}ms", System.currentTimeMillis() - start);
        return "component";
    }

    private List<UITaskMetric> getTaskData(MetricInfo info, String component, int window) {
        TreeMap<Integer, UITaskMetric> taskData = new TreeMap<>();
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                int taskId = JStormUtils.parseInt(UIMetricUtils.extractTaskId(split_name));
                String componentName = UIMetricUtils.extractComponentName(split_name);
                if (componentName != null && !componentName.equals(component)) continue;

                //only handle the tasks belongs to the specific component
                String metricName = UIMetricUtils.extractMetricName(split_name);

                String parentComp = null;
                if (metricName != null && metricName.contains(".")) {
                    parentComp = metricName.split("\\.")[0];
                    metricName = metricName.split("\\.")[1];
                }

                if (!metric.getValue().containsKey(window)) {
                    LOG.info("task snapshot {} missing window:{}", metric.getKey(), window);
                    continue;
                }
                MetricSnapshot snapshot = metric.getValue().get(window);

                UITaskMetric taskMetric;
                if (taskData.containsKey(taskId)) {
                    taskMetric = taskData.get(taskId);
                } else {
                    taskMetric = new UITaskMetric(component, taskId);
                    taskData.put(taskId, taskMetric);
                }
                taskMetric.setMetricValue(snapshot, parentComp, metricName);
            }
        }
        for (UITaskMetric t : taskData.values()) {
            t.mergeValue();
        }
        return new ArrayList<>(taskData.values());
    }


    private List<TaskEntity> getTaskEntities(TopologyInfo topologyInfo, String componentName) {
        TreeMap<Integer, TaskEntity> tasks = new TreeMap<>();

        for (ComponentSummary cs : topologyInfo.get_components()) {
            String compName = cs.get_name();
            String type = cs.get_type();
            if (componentName.equals(compName)) {
                for (int id : cs.get_taskIds()) {
                    tasks.put(id, new TaskEntity(id, compName, type));
                }
            }
        }

        for (TaskSummary ts : topologyInfo.get_tasks()) {
            if (tasks.containsKey(ts.get_taskId())) {
                TaskEntity te = tasks.get(ts.get_taskId());
                te.setHost(ts.get_host());
                te.setPort(ts.get_port());
                te.setStatus(ts.get_status());
                te.setUptime(ts.get_uptime());
                te.setErrors(ts.get_errors());
            }
        }

        return new ArrayList<>(tasks.values());
    }


    private ComponentSummary getComponentSummary(List<ComponentSummary> components, String compName) {
        for (ComponentSummary cs : components) {
            if (cs.get_name().equals(compName)) {
                return cs;
            }
        }
        return null;
    }

    private UIComponentMetric getComponentMetric(MetricInfo info, int window, String compName,
                                               List<ComponentSummary> componentSummaries) {
        UIComponentMetric compMetric = new UIComponentMetric(compName);
        if (info != null) {
            for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                String name = metric.getKey();
                String[] split_name = name.split("@");
                String componentName = UIMetricUtils.extractComponentName(split_name);
                if (componentName != null && !componentName.equals(compName)) continue;

                //only handle the specific component
                String metricName = UIMetricUtils.extractMetricName(split_name);
                String parentComp = null;
                if (metricName != null && metricName.contains(".")) {
                    parentComp = metricName.split("\\.")[0];
                    metricName = metricName.split("\\.")[1];
                }

                MetricSnapshot snapshot = metric.getValue().get(window);

                compMetric.setMetricValue(snapshot, parentComp, metricName);
            }
        }
        compMetric.mergeValue();
        ComponentSummary summary = getComponentSummary(componentSummaries, compName);
        compMetric.setParallel(summary.get_parallel());
        compMetric.setType(summary.get_type());
        return compMetric;
    }

}
