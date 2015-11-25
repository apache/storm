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

import com.alibaba.jstorm.ui.model.Response;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;
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
public class JStackController {
    private static final Logger LOG = LoggerFactory.getLogger(JStackController.class);

    @RequestMapping(value = "/jstack", method = RequestMethod.GET)
    public String show(@RequestParam(value = "cluster", required = true) String clusterName,
                       @RequestParam(value = "host", required = true) String host,
                       @RequestParam(value = "port", required = true) String logServerPort,
                       @RequestParam(value = "wport", required = true) String workerPort,
                       ModelMap model) {

        int i_logServerPort = JStormUtils.parseInt(logServerPort);
        int i_workerPort = JStormUtils.parseInt(workerPort);

        requestJStack(host, i_logServerPort, i_workerPort, model);
        model.addAttribute("clusterName", clusterName);
        return "jstack";
    }

    private void requestJStack(String host, int logServerPort, int workerPort, ModelMap model){
        String hostip = host + ":" +workerPort;
        Response res = UIUtils.getJStack(host, logServerPort, workerPort);
        String summary = null;
        String jstack = null;
        if(res.getStatus() > 0){
            if(res.getStatus() == 200){
                jstack = res.getData();
            }else{
                summary = "The JStack of <code>" + hostip + "</code> isn't exist <br/> " + res.getData();
            }
        }else{
            summary = "Failed to JStack of <code>" + hostip + "</code> <br/>" + res.getData();
        }
        model.addAttribute("jstack", jstack);
        model.addAttribute("summary", summary);
        model.addAttribute("hostip", hostip);
        UIUtils.addTitleAttribute(model, "JStack");

    }
}
