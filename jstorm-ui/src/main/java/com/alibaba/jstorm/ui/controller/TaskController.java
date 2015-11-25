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
import com.alibaba.jstorm.ui.model.UIStreamMetric;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class TaskController {
    private static final Logger LOG = LoggerFactory.getLogger(TaskController.class);

    @RequestMapping(value = "/task", method = RequestMethod.GET)
    public String show(@RequestParam(value = "cluster", required = true) String clusterName,
                       @RequestParam(value = "topology", required = true) String topology_id,
                       @RequestParam(value = "component", required = true) String component,
                       @RequestParam(value = "id", required = true) String task_id,
                       @RequestParam(value = "win", required = false) String win,
                       ModelMap model) {
        long start = System.currentTimeMillis();
        int window = UIUtils.parseWindow(win);
        UIUtils.addWindowAttribute(model, window);
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);

            //get task entity
            TopologyInfo topologyInfo = client.getClient().getTopologyInfo(topology_id);
            int id = JStormUtils.parseInt(task_id);
            TaskEntity task = getTaskEntity(topologyInfo.get_tasks(), id, component);
            model.addAttribute("task", task);

            //get task metric
            List<MetricInfo> taskStreamMetrics = client.getClient().getTaskAndStreamMetrics(topology_id, id);
//            System.out.println("taskMetrics size:"+getSize(taskMetrics));
            UITaskMetric taskMetric = getTaskMetric(taskStreamMetrics, component, id, window);
            model.addAttribute("taskMetric", taskMetric);
            model.addAttribute("taskHead", UIMetricUtils.sortHead(taskMetric, UITaskMetric.HEAD));

            //get stream metric
            List<UIStreamMetric> streamData = getStreamData(taskStreamMetrics, component, id, window);
            model.addAttribute("streamData", streamData);
            model.addAttribute("streamHead", UIMetricUtils.sortHead(streamData, UIStreamMetric.HEAD));

        } catch (NotAliveException nae) {
            model.addAttribute("flush", String.format("The topology: %s is dead", topology_id));
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            LOG.error(e.getMessage(), e);
            UIUtils.addErrorAttribute(model, e);
        }
        model.addAttribute("clusterName", clusterName);
        model.addAttribute("topologyId", topology_id);
        model.addAttribute("compName", component);
        model.addAttribute("page", "task");
        UIUtils.addTitleAttribute(model, "Task Summary");

        LOG.info("task page show cost:{}ms", System.currentTimeMillis() - start);
        return "task";
    }


    private int getSize(List<MetricInfo> infos) {
        int size = 0;
        for (MetricInfo info : infos) {
            size += info.get_metrics_size();
        }
        return size;
    }


    private TaskEntity getTaskEntity(List<TaskSummary> tasks, int id, String component) {
        TaskEntity entity = null;
        for (TaskSummary task : tasks) {
            if (task.get_taskId() == id) {
                entity = new TaskEntity(task);
                entity.setComponent(component);
                break;
            }
        }
        return entity;
    }


    private UITaskMetric getTaskMetric(List<MetricInfo> taskStreamMetrics, String component, int id, int window) {
        UITaskMetric taskMetric = new UITaskMetric(component, id);
        if (taskStreamMetrics.size() > 1) {
            MetricInfo info = taskStreamMetrics.get(0);
            if (info != null) {
                for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                    String name = metric.getKey();
                    String[] split_name = name.split("@");
                    int taskId = JStormUtils.parseInt(UIMetricUtils.extractTaskId(split_name));
                    if (taskId != id) continue;

                    //only handle the specific task
                    String metricName = UIMetricUtils.extractMetricName(split_name);

                    String parentComp = null;
                    if (metricName != null && metricName.contains(".")) {
                        parentComp = metricName.split("\\.")[0];
                        metricName = metricName.split("\\.")[1];
                    }

                    MetricSnapshot snapshot = metric.getValue().get(window);
                    taskMetric.setMetricValue(snapshot, parentComp, metricName);
                }
            }
        }
        taskMetric.mergeValue();
//        System.out.println("taskMetric:"+taskMetric);
        return taskMetric;
    }


    private List<UIStreamMetric> getStreamData(List<MetricInfo> taskStreamMetrics, String component, int id, int window) {
        Map<String, UIStreamMetric> streamData = new HashMap<>();
        if (taskStreamMetrics.size() > 1){
            MetricInfo info = taskStreamMetrics.get(1);
            if (info != null) {
                for (Map.Entry<String, Map<Integer, MetricSnapshot>> metric : info.get_metrics().entrySet()) {
                    String name = metric.getKey();
                    String[] split_name = name.split("@");
                    int taskId = JStormUtils.parseInt(UIMetricUtils.extractTaskId(split_name));
                    if (taskId != id) continue;

                    //only handle the specific task
                    String metricName = UIMetricUtils.extractMetricName(split_name);
                    String streamId = UIMetricUtils.extractStreamId(split_name);

                    String parentComp = null;
                    if (metricName != null && metricName.contains(".")) {
                        parentComp = metricName.split("\\.")[0];
                        metricName = metricName.split("\\.")[1];
                    }

                    MetricSnapshot snapshot = metric.getValue().get(window);

                    UIStreamMetric streamMetric;
                    if (streamData.containsKey(streamId)) {
                        streamMetric = streamData.get(streamId);
                    } else {
                        streamMetric = new UIStreamMetric(component, streamId);
                        streamData.put(streamId, streamMetric);
                    }
                    streamMetric.setMetricValue(snapshot, parentComp, metricName);
                }
            }
        }
        for (UIStreamMetric stream : streamData.values()) {
            stream.mergeValue();
        }
        return new ArrayList<>(streamData.values());
    }
}
