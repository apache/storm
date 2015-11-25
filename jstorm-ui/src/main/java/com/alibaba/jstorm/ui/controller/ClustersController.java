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

import com.alibaba.jstorm.ui.model.ClusterEntity;
import com.alibaba.jstorm.ui.utils.UIUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.Collection;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class ClustersController {

    private static final Logger LOG = LoggerFactory.getLogger(ClustersController.class);

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String show(ModelMap model) {
        UIUtils.readUiConfig();
        long start = System.currentTimeMillis();
        LOG.info("nimbus config: " + UIUtils.clusterConfig);
        Collection<ClusterEntity> clusterEntities = UIUtils.clustersCache.values();
        model.addAttribute("clusters", clusterEntities);
        model.addAttribute("nodes", getNodeCount(clusterEntities));
        UIUtils.addTitleAttribute(model, null);
        LOG.info("clusters page show cost:{}ms", System.currentTimeMillis() - start);
        return "clusters";
    }

    private int getNodeCount(Collection<ClusterEntity> clusterEntities){
        int count = 0;
        for (ClusterEntity c : clusterEntities){
            count += c.getSupervisor_num();
        }
        return count;
    }

}
