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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.NimbusClient;

import com.alibaba.jstorm.ui.NimbusClientManager;
import com.alibaba.jstorm.ui.UIDef;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ColumnData;
import com.alibaba.jstorm.ui.model.PageGenerator;
import com.alibaba.jstorm.ui.model.TableData;
import com.alibaba.jstorm.utils.HttpserverUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

public class ConfPage implements PageGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(ConfPage.class);

    public String getSupervisorConf(Map<String, String> paramMap)
            throws ClientProtocolException, IOException {
        String host = paramMap.get(UIDef.HOST);
        String port = paramMap.get(UIDef.PORT);

        if (host == null || port == null) {
            throw new IllegalArgumentException(
                    "Invalid parameter, please set supervisor host and port");
        }

        final String PROXY_URL = "http://%s:%s/logview?%s=%s";
        String baseUrl =
                String.format(PROXY_URL, NetWorkUtils.host2Ip(host), port,
                        HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD,
                        HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_SHOW_CONF);
        String url = baseUrl;

        // 1. proxy call the task host log view service
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);
        HttpResponse response = client.execute(post);

        // 2. check the request is success, then read the log
        if (response.getStatusLine().getStatusCode() == 200) {
            String data = EntityUtils.toString(response.getEntity(), "utf-8");

            return data;
        } else {
            return (EntityUtils.toString(response.getEntity()));
        }
    }

    public String getConf(Map<String, String> paramMap) {
        NimbusClient client = null;
        try {
            client = NimbusClientManager.getNimbusClient(paramMap);

            String confType = paramMap.get(UIDef.CONF_TYPE);
            if (UIDef.CONF_TYPE_NIMBUS.equals(confType)) {
                String confStr = client.getClient().getNimbusConf();
                return confStr;
            } else if (UIDef.CONF_TYPE_TOPOLOGY.equals(confType)) {
                String topologyId = paramMap.get(UIDef.TOPOLOGY);
                if (topologyId == null) {
                    throw new IllegalArgumentException(
                            "Invalid parameter, please set topologyId");
                }

                return client.getClient().getTopologyConf(topologyId);
            } else if (UIDef.CONF_TYPE_SUPERVISOR.equals(confType)) {
                return getSupervisorConf(paramMap);
            } else {
                throw new IllegalArgumentException(
                        "Invalid parameter, please Conf type");
            }

        } catch (Exception e) {
            NimbusClientManager.removeClient(paramMap);
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } 
    }

    public TableData getTable(Map<String, Object> conf) {
        TableData table = new TableData();
        List<String> headers = table.getHeaders();
        List<Map<String, ColumnData>> lines = table.getLines();

        table.setName(UIDef.HEADER_CONF);

        final String headerKey = "Key";
        final String headerValue = "value";
        headers.add(headerKey);
        headers.add(headerValue);

        for (Entry<String, Object> entry : conf.entrySet()) {
            Map<String, ColumnData> line = new HashMap<String, ColumnData>();
            lines.add(line);

            ColumnData keyColumn = new ColumnData();
            keyColumn.addText(entry.getKey());
            line.put(headerKey, keyColumn);

            ColumnData valueColumn = new ColumnData();
            valueColumn.addText(String.valueOf(entry.getValue()));
            line.put(headerValue, valueColumn);
        }

        return table;
    }

    @Override
    public Output generate(Map<String, String> paramMap) {
        // TODO Auto-generated method stub
        List<TableData> tables = new ArrayList<TableData>();

        try {
            String confStr = getConf(paramMap);
            Map<String, Object> conf =
                    (Map<String, Object>) JStormUtils.from_json(confStr);

            TableData table = getTable(conf);
            tables.add(table);

            Output ret = new Output();
            ret.tables = tables;
            ret.rawData = "";
            return ret;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);

            return UIUtils.getErrorInfo(e);
        }

    }

}
