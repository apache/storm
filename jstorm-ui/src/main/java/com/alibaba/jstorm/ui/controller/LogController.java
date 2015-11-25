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
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.ui.model.Response;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
@Controller
public class LogController {
    private static final Logger LOG = LoggerFactory.getLogger(LogController.class);

    @RequestMapping(value = "/log", method = RequestMethod.GET)
    public String show(@RequestParam(value = "cluster", required = true) String clusterName,
                       @RequestParam(value = "host", required = true) String host,
                       @RequestParam(value = "port", required = false) String logServerPort,
                       @RequestParam(value = "dir", required = false) String dir,
                       @RequestParam(value = "wport", required = false) String workerPort,
                       @RequestParam(value = "file", required = false) String logName,
                       @RequestParam(value = "tid", required = false) String topologyId,
                       @RequestParam(value = "pos", required = false) String pos,
                       ModelMap model) {
        if (StringUtils.isBlank(dir)) {
            dir = ".";
        }
        Map conf = UIUtils.getNimbusConf(clusterName);
        Event event = new Event(clusterName, host, logServerPort, logName, topologyId, workerPort, pos, dir, conf);

        try {
            requestLog(event, model);
        } catch (IOException e) {
            e.printStackTrace();
        }


        model.addAttribute("logName", event.logName);
        model.addAttribute("host", host);
        model.addAttribute("dir", event.dir);
        model.addAttribute("clusterName", clusterName);
        return "log";
    }


    private long getCurrentPageIndex(Event e, long totalSize, int pageSize) {
        long currentPos = totalSize;
        if (e.pos >= 0) {
            currentPos = e.pos;
        }

        return currentPos / pageSize;
    }

    private Pagination createPage(Event e, long index, int pageSize, String text,
                                  boolean isDisable, boolean isActive) {
        long pos = index * pageSize;
        String url = String.format("log?cluster=%s&host=%s&port=%s&file=%s&pos=%s&dir=%s",
                e.clusterName, e.host, e.logServerPort, e.logName, pos, e.dir);

        Pagination page = new Pagination();
        page.url = url;
        page.text = text;
        if (isDisable) {
            page.status = "disabled";
        } else if (isActive) {
            page.status = "active";
        }
        return page;
    }

    private List<Pagination> genPageUrl(Event e, String sizeStr) {
        long totalSize = Long.valueOf(sizeStr);
        int pageSize = e.logPageSize;

        long pageNum = (totalSize + pageSize - 1) / pageSize;
        long currentPageIndex = getCurrentPageIndex(e, totalSize, pageSize);

        List<Pagination> pages = new ArrayList<>();
        if (pageNum <= 10) {
            for (long i = pageNum - 1; i >= 0; i--) {
                pages.add(createPage(e, i, pageSize, String.valueOf(i + 1), false, i == currentPageIndex));
            }
            return pages;
        }

        if (pageNum - currentPageIndex < 5) {
            for (long i = pageNum - 1; i >= currentPageIndex; i--) {
                pages.add(createPage(e, i, pageSize, String.valueOf(i + 1), false, i == currentPageIndex));
            }
        } else {
            pages.add(createPage(e, pageNum - 1, pageSize, "End", false, pageNum - 1 == currentPageIndex));
            pages.add(createPage(e, currentPageIndex + 4, pageSize, "...", false, false));
            for (long i = currentPageIndex + 3; i >= currentPageIndex; i--) {
                pages.add(createPage(e, i, pageSize, String.valueOf(i + 1), false, i == currentPageIndex));
            }
        }

        if (currentPageIndex < 5) {
            for (long i = currentPageIndex - 1; i > 0; i--) {
                pages.add(createPage(e, i, pageSize, String.valueOf(i + 1), false, i == currentPageIndex));
            }
        } else {
            for (long i = currentPageIndex - 1; i >= currentPageIndex - 3; i--) {
                pages.add(createPage(e, i, pageSize, String.valueOf(i + 1), false, i == currentPageIndex));
            }
            pages.add(createPage(e, currentPageIndex - 4, pageSize, "...", false, false));
            pages.add(createPage(e, 0, pageSize, "Begin", false, 0 == currentPageIndex));
        }
        return pages;
    }

    private void requestLog(Event e, ModelMap model) throws IOException {
        String fullPath;
        if (e.dir == null || e.dir.equals(".")) {
            fullPath = e.logName;
        } else {
            fullPath = e.dir + File.separator + e.logName;
        }
        String summary = null;
        String log = null;
        if (fullPath.contains("/..") || fullPath.contains("../")){
            summary = "File Path can't contains <code>..</code> <br/>";
        }else {
            Response res = UIUtils.getLog(e.host, e.logServerPort, fullPath, e.pos);

            if (res.getStatus() != -1) {
                if (res.getStatus() == 200) {
                    String sizeStr = res.getData().substring(0, 16);
                    model.addAttribute("pages", genPageUrl(e, sizeStr));
                    log = res.getData().substring(17);
                } else {
                    summary = "The log file <code>" + e.logName + "</code> isn't exist <br/> " + res.getData();
                }
            } else {
                summary = "Failed to get log file <code>" + e.logName + "</code> <br/>" + res.getData();
            }
        }
        model.addAttribute("log", log);
        model.addAttribute("summary", summary);
        UIUtils.addTitleAttribute(model, "Log");
    }


    public class Event {

        public String clusterName;
        public String host;
        public int logServerPort;
        public String logName;
        public String topologyId;
        public String workerPort;
        public long pos;
        public String dir;
        public int logPageSize;
        public String logEncoding;


        public Event(String _clusterName, String _host, String _logServerPort, String _logName,
                     String _topologyId, String _workerPort, String _pos, String _dir, Map conf) {
            this.clusterName = _clusterName;
            this.host = _host;
            this.logServerPort = JStormUtils.parseInt(_logServerPort, 0);
            this.logName = _logName;
            this.topologyId = _topologyId;
            this.workerPort = _workerPort;
            this.pos = JStormUtils.parseLong(_pos, -1);
            this.dir = _dir;

            logPageSize = ConfigExtension.getLogPageSize(conf);
            logEncoding = ConfigExtension.getLogViewEncoding(conf);

            if (host == null) {
                throw new IllegalArgumentException("Please set host");
            } else if (logServerPort == 0) {
                throw new IllegalArgumentException(
                        "Please set log server's port");
            }

            if (!StringUtils.isBlank(logName)) {
                return;
            }

            if (topologyId == null || workerPort == null) {
                throw new IllegalArgumentException(
                        "Please set log fileName or topologyId-workerPort");
            }

            String topologyName;
            try {
                topologyName = Common.topologyIdToName(topologyId);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Please set log fileName or topologyId-workerPort");
            }
            dir = "." + File.separator + topologyName;
            logName = topologyName + "-worker-" + workerPort + ".log";
        }
    }


    public class Pagination {
        public String status;
        public String url;
        public String text;

        public Pagination(String status, String url, String text) {
            this.status = status;
            this.url = url;
            this.text = text;
        }

        public Pagination() {
        }

        public String getStatus() {
            return status;
        }

        public String getUrl() {
            return url;
        }

        public String getText() {
            return text;
        }
    }
}
