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

import com.alibaba.jstorm.ui.utils.UIUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class ConfController {
    private static final Logger LOG = LoggerFactory.getLogger(ConfController.class);

    @RequestMapping(value = "/conf", method = RequestMethod.GET)
    public String show(@RequestParam(value = "name", required = true) String name,
                       @RequestParam(value = "type", required = false) String type,
                       @RequestParam(value = "topology", required = false) String topologyId,
                       @RequestParam(value = "host", required = false) String host, ModelMap model) {
        String title = null;
        String uri = String.format("api/v1/cluster/%s", name);
        if (!StringUtils.isBlank(type)){
            if (type.equals("supervisor")) {
                uri += String.format("/supervisor/%s/configuration", host);
                title = "Supervisor Conf";
                model.addAttribute("subtitle", host);
                model.addAttribute("host", host);
            }else if(type.equals("topology")){
                uri += String.format("/topology/%s/configuration", topologyId);
                title = "Topology Conf";
                model.addAttribute("subtitle", topologyId);
                model.addAttribute("topologyId", topologyId);
            }
        } else {
            uri += "/configuration";
            title = "Nimbus Conf";
            model.addAttribute("subtitle", name);
        }
        model.addAttribute("clusterName", name);
        model.addAttribute("uri", uri);
        model.addAttribute("pageTitle", title);
        model.addAttribute("page", "conf");
        UIUtils.addTitleAttribute(model, "Configuration");
        return "conf";
    }

}
