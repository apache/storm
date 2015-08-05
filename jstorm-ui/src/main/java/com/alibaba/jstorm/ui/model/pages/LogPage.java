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
package com.alibaba.jstorm.ui.model.pages;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.ui.NimbusClientManager;
import com.alibaba.jstorm.ui.UIDef;
import com.alibaba.jstorm.ui.UIMetrics;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.LinkData;
import com.alibaba.jstorm.ui.model.PageIndex;
import com.alibaba.jstorm.utils.HttpserverUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

@ManagedBean(name = "logpage")
@ViewScoped
public class LogPage implements Serializable {

    private static final long serialVersionUID = 4326599394273506083L;

    private static final Logger LOG = LoggerFactory.getLogger(LogPage.class);

    /**
     * proxy url, which call the log service on the task node.
     */
    private static final String PROXY_URL = "http://%s:%s/logview?%s=%s&log=%s";

    /**
     * store the log content.
     */
    private String log = "";

    private List<PageIndex> pages = new ArrayList<PageIndex>();

    protected Map<String, String> paramMap;

    protected LogPage.Event event;

    protected Map<String, Object> conf;

    public LogPage() throws Exception {

        FacesContext ctx = FacesContext.getCurrentInstance();
        paramMap = ctx.getExternalContext().getRequestParameterMap();

        init();
    }

    public LogPage(Map<String, String> paramMap) throws Exception {
        this.paramMap = paramMap;

        init();
    }

    private void init() throws Exception {

        long start = System.nanoTime();
        try {
            event = new LogPage.Event(paramMap);

            conf = getNimbusConf();

            // proxy call
            queryLog(event);

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }finally {
            long end = System.nanoTime();
            UIMetrics.updateHistorgram(this.getClass().getSimpleName(), (end - start)/1000000.0d);
        
        }
    }

    private Map<String, Object> getNimbusConf() {
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(paramMap);

            String jsonConf = client.getClient().getNimbusConf();
            Map<String, Object> nimbusConf =
                    (Map<String, Object>) Utils.from_json(jsonConf);
            return nimbusConf;
        } catch (Exception e) {
            NimbusClientManager.removeClient(paramMap);
            LOG.error(e.getMessage(), e);
            return UIUtils.readUiConfig();
        } 
    }

    

    /**
     * proxy query log for the specified task.
     * 
     * @param task the specified task
     * @throws IOException
     * @throws ClientProtocolException
     */
    private void queryLog(LogPage.Event event) throws ClientProtocolException,
            IOException {
        // PROXY_URL = "http://%s:%s/logview?%s=%s&log=%s";
        String baseUrl =
                String.format(PROXY_URL, NetWorkUtils.host2Ip(event.host),
                        event.logServerPort,
                        HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD,
                        HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_SHOW,
                        event.logName);
        String url = baseUrl;
        if (event.pos != null) {
            url +=
                    ("&" + HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_POS + "=" + event.pos);
        }

        // 1. proxy call the task host log view service
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);
        HttpResponse response = client.execute(post);

        // 2. check the request is success, then read the log
        if (response.getStatusLine().getStatusCode() == 200) {
            String data =
                    EntityUtils.toString(response.getEntity(),
                            ConfigExtension.getLogViewEncoding(conf));

            String sizeStr = data.substring(0, 16);
            
            
            PageIndex.Event pageIndexEvent = new PageIndex.Event();
            pageIndexEvent.totalSize = Long.valueOf(sizeStr);
            pageIndexEvent.pos = JStormUtils.parseLong(event.pos, pageIndexEvent.totalSize > 0 ? pageIndexEvent.totalSize - 1: 0);
            pageIndexEvent.pageSize = ConfigExtension.getLogPageSize(conf); 
            pageIndexEvent.url = UIDef.LINK_LOG;
            pageIndexEvent.paramMap = event.toMap();
            
            pages = PageIndex.generatePageIndex(pageIndexEvent);

            setLog(data);
        } else {
            setLog(EntityUtils.toString(response.getEntity()));
        }

    }


    public static class Event implements Serializable {

        public final String clusterName;
        public final String host;
        public final String logServerPort;
        public String logName;
        public final String topologyId;
        public final String workerPort;
        public final String pos;

        public Event(Map<String, String> paramMap) {
            clusterName = paramMap.get(UIDef.CLUSTER);
            host = paramMap.get(UIDef.HOST);
            logServerPort = paramMap.get(UIDef.LOG_SERVER_PORT);
            logName = paramMap.get(UIDef.LOG_NAME);
            topologyId = paramMap.get(UIDef.TOPOLOGY);
            workerPort = paramMap.get(UIDef.PORT);
            pos = paramMap.get(UIDef.POS);

            if (host == null) {
                throw new IllegalArgumentException("Please set host");
            } else if (logServerPort == null) {
                throw new IllegalArgumentException(
                        "Please set log server's port");
            }

            if (StringUtils.isBlank(logName) == false) {
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
                // TODO Auto-generated catch block
                throw new IllegalArgumentException(
                        "Please set log fileName or topologyId-workerPort");
            }
            logName = topologyName + "-worker-" + workerPort + ".log";

        }
        
        public Map<String, String> toMap() {
            Map<String, String> paramMap = new HashMap<String, String>();
            
            paramMap.put(UIDef.CLUSTER, clusterName);
            paramMap.put(UIDef.HOST, host);
            paramMap.put(UIDef.LOG_SERVER_PORT, logServerPort);
            paramMap.put(UIDef.LOG_NAME, logName);
            paramMap.put(UIDef.POS, pos);
            
            return paramMap;
        }

        public String getClusterName() {
            return clusterName;
        }

        public String getHost() {
            return host;
        }

        public String getLogServerPort() {
            return logServerPort;
        }

        public String getLogName() {
            return logName;
        }

        public String getTopologyId() {
            return topologyId;
        }

        public String getWorkerPort() {
            return workerPort;
        }

        public String getPos() {
            return pos;
        }

    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public List<PageIndex> getPages() {
        return pages;
    }

    public Event getEvent() {
        return event;
    }

    public Map<String, Object> getConf() {
        return conf;
    }

    public static void main() {

    }
}
