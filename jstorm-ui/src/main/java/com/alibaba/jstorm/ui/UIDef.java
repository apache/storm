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
package com.alibaba.jstorm.ui;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.jstorm.ui.model.PageGenerator;
import com.alibaba.jstorm.ui.model.pages.ClusterPage;
import com.alibaba.jstorm.ui.model.pages.ComponentPage;
import com.alibaba.jstorm.ui.model.pages.ConfPage;
import com.alibaba.jstorm.ui.model.pages.JStackPage;
import com.alibaba.jstorm.ui.model.pages.ListLogsPage;
import com.alibaba.jstorm.ui.model.pages.NettyPage;
import com.alibaba.jstorm.ui.model.pages.SupervisorPage;
import com.alibaba.jstorm.ui.model.pages.TopologyPage;

public class UIDef {
    public static String LINK_CLUSTER_INFO = "";

    public static final String LINK_TABLE_PAGE = "table.jsf";
    public static final String LINK_WINDOW_TABLE = "windowtable.jsf";
    public static final String LINK_LOG = "log.jsf";

    public static final String PAGE_TYPE = "pageType";

    public static final String PAGE_TYPE_CLUSTER = "cluster";
    public static final String PAGE_TYPE_SUPERVISOR = "supervisor";
    public static final String PAGE_TYPE_TOPOLOGY = "topology";
    public static final String PAGE_TYPE_COMPONENT = "component";
    public static final String PAGE_TYPE_CONF = "conf";
    public static final String PAGE_TYPE_LOG = "log";
    public static final String PAGE_TYPE_LISTLOG = "listLog";
    public static final String PAGE_TYPE_JSTACK = "jstack";
    public static final String PAGE_TYPE_NETTY = "netty";

    public static final String CONF_TYPE = "confType";
    public static final String CONF_TYPE_NIMBUS = "nimbus";
    public static final String CONF_TYPE_SUPERVISOR = "supervisor";
    public static final String CONF_TYPE_TOPOLOGY = "topology";

    public static final String HEADER_SUPERVISOR = "Supervisor Num";
    public static final String HEADER_TOTAL_PORT = "Total Ports";
    public static final String HEADER_USED_PORT = "Used Ports";
    public static final String HEADER_FREE_PORT = "Free Ports";
    public static final String HEADER_VERSION = "Version";
    public static final String HEADER_CONF = "Configurations";
    public static final String HEADER_LOGS = "Logs";
    public static final String HEADER_LOG = "Log";
    public static final String HEADER_UPTIME = "Uptime";
    public static final String HEADER_TOPOLOGY_NAME = "Topology Name";
    public static final String HEADER_TOPOLOGY_ID = "Topology ID";
    public static final String HEADER_TOPOLOGY_METRICS = "Topology Metrics";
    public static final String HEADER_STATUS = "Status";
    public static final String HEADER_WORKER_NUM = "Worker Num";
    public static final String HEADER_TASK_NUM = "Task Num";
    public static final String HEADER_TASK_LIST = "Task List";
    public static final String HEADER_TASK_ID = "Task Id";
    public static final String HEADER_COMPONENT_NAME = "Component Name";
    public static final String HEADER_COMPONENT_TYPE = "Component Type";
    public static final String HEADER_ERROR = "Errors";
    public static final String HEADER_STREAM_ID = "Stream Id";
    public static final String HEADER_NETWORKER_CONNECTION = "NetworkConnection";
    

    public static final String CLUSTER = "cluster";
    public static final String HOST = "host";
    public static final String IP = "IP";
    public static final String PORT = "port";
    public static final String TOPOLOGY = "topology";
    public static final String TOPOLOGY_NAME = "topologyName";
    public static final String SUPERVISOR = "supervisor";
    public static final String DIR = "dir";
    public static final String WOKER = "worker";
    public static final String COMPONENT = "component";
    public static final String JSTACK = "jstack";
    public static final String METRICS = "metrics";
    public static final String WINDOW = "window";
    public static final String BOLT = "bolt";
    public static final String SPOUT = "spout";
    public static final String LOG_NAME = "logName";
    public static final String LOG_SERVER_PORT = "logServerPort";
    public static final String POS = "pos";

    public static final Map<String, PageGenerator> pageGeneratos =
            new HashMap<String, PageGenerator>();
    static {
        pageGeneratos.put(PAGE_TYPE_CLUSTER, new ClusterPage());
        pageGeneratos.put(PAGE_TYPE_SUPERVISOR, new SupervisorPage());
        pageGeneratos.put(PAGE_TYPE_TOPOLOGY, new TopologyPage());
        pageGeneratos.put(PAGE_TYPE_COMPONENT, new ComponentPage());
        pageGeneratos.put(PAGE_TYPE_CONF, new ConfPage());
        pageGeneratos.put(PAGE_TYPE_JSTACK, new JStackPage());
        pageGeneratos.put(PAGE_TYPE_LISTLOG, new ListLogsPage());
        pageGeneratos.put(PAGE_TYPE_NETTY, new NettyPage());

    }

}
