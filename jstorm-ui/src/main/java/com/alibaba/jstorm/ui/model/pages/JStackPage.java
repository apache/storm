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
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.ui.UIDef;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.PageGenerator;
import com.alibaba.jstorm.ui.model.TableData;
import com.alibaba.jstorm.utils.HttpserverUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

public class JStackPage implements PageGenerator {
    private static final long serialVersionUID = 4326599394273506085L;

    private static final Logger LOG = LoggerFactory.getLogger(JStackPage.class);

    protected static class Event {
        public String clusterName;
        public String host;
        public String topologyId;
        public String workerport;
        public String logServerPort;

        public Event(Map<String, String> paramMap) {
            clusterName = paramMap.get(UIDef.CLUSTER);
            host = paramMap.get(UIDef.HOST);
            workerport = paramMap.get(UIDef.PORT);
            topologyId = paramMap.get(UIDef.TOPOLOGY);
            logServerPort = paramMap.get(UIDef.LOG_SERVER_PORT);

            if (StringUtils.isBlank(host)) {
                throw new IllegalArgumentException("Please set host");
            } else if (StringUtils.isBlank(workerport)) {
                throw new IllegalArgumentException("Please set port");
            } else if (StringUtils.isBlank(logServerPort)) {
                throw new IllegalArgumentException("Please set dir");
            }
        }
    }

    public String getErrMsg(Event event) {
        StringBuilder sb = new StringBuilder();
        sb.append(event.topologyId);
        sb.append("'s worker on ");
        sb.append(event.host);
        sb.append(":");
        sb.append(event.workerport);
        sb.append(" disappear.");

        return sb.toString();
    }

    public String getJStack(Event event) throws ClientProtocolException,
            IOException {
        final String PROXY_URL = "http://%s:%s/logview?%s=%s&%s=%s";
        String baseUrl =
                String.format(PROXY_URL, NetWorkUtils.host2Ip(event.host),
                        event.logServerPort,
                        HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD,
                        HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_JSTACK,
                        HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_WORKER_PORT,
                        event.workerport);
        String url = baseUrl;
        // 1. proxy call the task host log view service
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);
        HttpResponse response = client.execute(post);

        String errMsg = getErrMsg(event);

        if (response.getStatusLine().getStatusCode() == 200) {
            String data = EntityUtils.toString(response.getEntity());

            if (StringUtils.isBlank(data)) {
                throw new IllegalArgumentException(errMsg);
            }

            return data;
        } else {
            throw new IllegalArgumentException(errMsg);
        }

    }

    @Override
    public Output generate(Map<String, String> paramMap) {
        // TODO Auto-generated method stub

        try {
            Event event = new Event(paramMap);

            String rawData = getJStack(event);

            Output output = new Output();
            output.tables = new ArrayList<TableData>();
            output.rawData = rawData;
            return output;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);

            return UIUtils.getErrorInfo(e);
        }

    }

}
