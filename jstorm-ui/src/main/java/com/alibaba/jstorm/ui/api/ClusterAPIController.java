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
package com.alibaba.jstorm.ui.api;

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.ui.model.graph.ChartSeries;
import com.alibaba.jstorm.ui.utils.NimbusClientManager;
import com.alibaba.jstorm.ui.utils.UIDef;
import com.alibaba.jstorm.ui.utils.UIUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@RestController
@RequestMapping(UIDef.API_V1 + "/cluster/{clusterName}")
public class ClusterAPIController {
    private final static Logger LOG = LoggerFactory.getLogger(ClusterAPIController.class);

    @RequestMapping("/summary/metrics")
    public List<ChartSeries> summaryMetrics(@PathVariable String clusterName,
                                            @RequestParam(value = "win", required = false) String win) {
        int window = UIUtils.parseWindow(win);
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(clusterName);
            List<MetricInfo> infos = client.getClient().getMetrics(JStormMetrics.CLUSTER_METRIC_KEY,
                    MetaType.TOPOLOGY.getT());
            return UIUtils.getChartSeries(infos, window);
        } catch (Exception e) {
            NimbusClientManager.removeClient(clusterName);
            LOG.error(e.getMessage(), e);
        }
        return null;
    }
}