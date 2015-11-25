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

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.ui.model.Response;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.FileAttribute;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class FilesController {
    private static final Logger LOG = LoggerFactory.getLogger(FilesController.class);

    /**
     * proxy url, which call the log service on the task node.
     */
    private static final String PROXY_URL = "http://%s:%s/logview?%s=%s&%s=%s";


    private List<FileAttribute> files = new ArrayList<FileAttribute>();

    private List<FileAttribute> dirs = new ArrayList<FileAttribute>();

    @RequestMapping(value = "/files", method = RequestMethod.GET)
    public String show(@RequestParam(value = "cluster", required = true) String cluster_name,
                       @RequestParam(value = "host", required = true) String host,
                       @RequestParam(value = "port", required = false) String port,
                       @RequestParam(value = "dir", required = false) String dir,
                       ModelMap model) {
        dirs.clear();
        files.clear();
        Map conf = UIUtils.readUiConfig();
        if (StringUtils.isBlank(dir)) {
            dir = ".";
        }

        String[] path = dir.split("/");
        model.addAttribute("path", path);

        int i_port;
        if (StringUtils.isBlank(port)) {
            i_port = ConfigExtension.getNimbusDeamonHttpserverPort(conf);
        } else {
            i_port = JStormUtils.parseInt(port);
        }

        //proxy request for files info
        String summary = requestFiles(host, i_port, dir);

        model.addAttribute("summary", summary);
        model.addAttribute("files", files);
        model.addAttribute("dirs", dirs);

        // status save
        model.addAttribute("clusterName", cluster_name);
        model.addAttribute("host", host);
        model.addAttribute("port", i_port);
        model.addAttribute("parent", dir);
        UIUtils.addTitleAttribute(model, "Files");

        return "files";
    }


    private String requestFiles(String host, int port, String dir){
        if (dir.contains("/..") || dir.contains("../")){
            return "File Path can't contains <code>..</code> <br/>";
        }
        Response response = UIUtils.getFiles(host, port, dir);
        String summary = null;
        if(response.getStatus() > 0){
            if(response.getStatus() == 200){
                parseString(response.getData());
            }else{
                summary = "The directory <code>" + dir + "</code> isn't exist <br/> " + response.getData();
            }
        }else{
            summary = "Failed to get files <code>" + dir + "</code> <br/>" + response.getData();
        }
        return summary;
    }


    private void parseString(String input) {
        Map<String, Map> map = (Map<String, Map>) JStormUtils
                .from_json(input);
        for (Map jobj : map.values()) {
            FileAttribute attribute = FileAttribute.fromJSONObject(jobj);
            if (attribute != null) {

                if (JStormUtils.parseBoolean(attribute.getIsDir(), false) == true) {
                    dirs.add(attribute);
                } else {
                    files.add(attribute);
                }

            }

        }
    }
}
