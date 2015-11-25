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

import backtype.storm.utils.Utils;
import com.alibaba.jstorm.ui.utils.UIDef;
import com.alibaba.jstorm.ui.utils.UIUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@RestController
@RequestMapping(UIDef.API_V1 + "/cluster/{name}")
public class ConfAPIController {
    private final static Logger LOG = LoggerFactory.getLogger(ConfAPIController.class);

    @RequestMapping("/configuration")
    public Map nimbusConf(@PathVariable String name) {
        return UIUtils.getNimbusConf(name);
    }

    @RequestMapping("/supervisor/{host}/configuration")
    public Map supervisorConf(@PathVariable String name, @PathVariable String host) {
        int port = UIUtils.getSupervisorPort(name);
        String json = UIUtils.getSupervisorConf(host, port).getData();
        return (Map) Utils.from_json(json);
    }

    @RequestMapping("/topology/{topology}/configuration")
    public Map topologyConf(@PathVariable String name, @PathVariable String topology) {
        return UIUtils.getTopologyConf(name, topology);
    }

}
