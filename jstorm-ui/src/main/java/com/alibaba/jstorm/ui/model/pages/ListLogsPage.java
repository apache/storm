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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import com.alibaba.jstorm.ui.model.ColumnData;
import com.alibaba.jstorm.ui.model.LinkData;
import com.alibaba.jstorm.ui.model.PageGenerator;
import com.alibaba.jstorm.ui.model.TableData;
import com.alibaba.jstorm.utils.FileAttribute;
import com.alibaba.jstorm.utils.HttpserverUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

public class ListLogsPage implements PageGenerator {
    private static final long serialVersionUID = 4326599394273506085L;

    private static final Logger LOG = LoggerFactory
            .getLogger(ListLogsPage.class);

    protected static class Event {
        public String clusterName;
        public String host;
        public String port;
        public String dir;

        public Event(Map<String, String> paramMap) {
            String host = paramMap.get(UIDef.HOST);
            String port = paramMap.get(UIDef.PORT);
            String dir = paramMap.get(UIDef.DIR);
            String clusterName = paramMap.get(UIDef.CLUSTER);

            if (StringUtils.isBlank(host)) {
                throw new IllegalArgumentException("Please set host");
            } else if (StringUtils.isBlank(port)) {
                throw new IllegalArgumentException("Please set port");
            } else if (StringUtils.isBlank(dir)) {
                throw new IllegalArgumentException("Please set dir");
            }

            this.host = host;
            this.port = port;
            this.dir = dir;
            this.clusterName = clusterName;
        }

    }

    private List<TableData> parseString(String input, ListLogsPage.Event event) {
        Map<String, Map<String, String>> map =
                (Map<String, Map<String, String>>) JStormUtils.from_json(input);

        List<TableData> ret = new ArrayList<TableData>();
        TableData dirTable = new TableData();
        TableData fileTable = new TableData();
        ret.add(dirTable);
        ret.add(fileTable);

        dirTable.setName("Dirs");
        fileTable.setName("Files");

        Set<String> keys = new HashSet<String>();

        for (Map<String, String> jobj : map.values()) {
            keys.addAll(jobj.keySet());

        }

        List<String> dirHeaders = dirTable.getHeaders();
        dirHeaders.addAll(keys);
        List<String> fileHeaders = fileTable.getHeaders();
        fileHeaders.addAll(keys);

        for (Map<String, String> jobj : map.values()) {
            boolean isDir = false;
            Map<String, ColumnData> line = new HashMap<String, ColumnData>();

            String fileName = null;
            ColumnData linkColumn = null;
            for (Entry<String, String> entry : jobj.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                if (FileAttribute.FILE_NAME_FIELD.equals(key)) {
                    fileName = value;
                } else if (FileAttribute.IS_DIR_FIELD.equals(key)) {
                    isDir = Boolean.valueOf(value);

                    ColumnData columnData = new ColumnData();
                    columnData.addText(value);
                    line.put(key, columnData);
                } else {

                    ColumnData columnData = new ColumnData();
                    columnData.addText(value);
                    line.put(key, columnData);
                }
            }

            if (isDir == true) {
                ColumnData column = new ColumnData();
                line.put(FileAttribute.FILE_NAME_FIELD, column);
                LinkData linkData = new LinkData();
                column.addLinkData(linkData);

                linkData.setUrl(UIDef.LINK_TABLE_PAGE);
                linkData.setText(fileName);
                linkData.addParam(UIDef.CLUSTER, event.clusterName);
                linkData.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_LISTLOG);
                linkData.addParam(UIDef.HOST, event.host);
                linkData.addParam(UIDef.PORT, event.port);
                linkData.addParam(UIDef.DIR, event.dir + File.separator
                        + fileName);

                dirTable.getLines().add(line);
            } else {
                ColumnData column = new ColumnData();
                line.put(FileAttribute.FILE_NAME_FIELD, column);
                LinkData linkData = new LinkData();
                column.addLinkData(linkData);

                linkData.setUrl(UIDef.LINK_LOG);
                linkData.setText(fileName);
                linkData.addParam(UIDef.CLUSTER, event.clusterName);
                linkData.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_LOG);
                linkData.addParam(UIDef.HOST, event.host);
                linkData.addParam(UIDef.LOG_SERVER_PORT, event.port);
                linkData.addParam(UIDef.LOG_NAME, event.dir + File.separator
                        + fileName);
                fileTable.getLines().add(line);
            }

        }
        return ret;
    }

    /**
     * proxy query log for the specified task.
     * 
     * @param task the specified task
     * @throws IOException
     * @throws ClientProtocolException
     */
    private List<TableData> listLogs(ListLogsPage.Event event)
            throws ClientProtocolException, IOException {

        final String PROXY_URL = "http://%s:%s/logview?%s=%s&%s=%s";
        // PROXY_URL = "http://%s:%s/logview?%s=%s&dir=%s";
        String url =
                String.format(PROXY_URL, NetWorkUtils.host2Ip(event.host),
                        event.port,
                        HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD,
                        HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_LIST,
                        HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_DIR, event.dir);

        // 1. proxy call the task host log view service
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);
        HttpResponse response = client.execute(post);

        // 2. check the request is success, then read the log
        if (response.getStatusLine().getStatusCode() == 200) {
            String data = EntityUtils.toString(response.getEntity());

            return parseString(data, event);

        } else {
            String data = EntityUtils.toString(response.getEntity());
            List<TableData> ret = new ArrayList<TableData>();
            ret.add(UIUtils.errorTable("Failed to list dir"));
            return ret;
        }

    }

    @Override
    public Output generate(Map<String, String> paramMap) {
        // TODO Auto-generated method stub
        ListLogsPage.Event event = new ListLogsPage.Event(paramMap);

        try {
            List<TableData> tables = listLogs(event);

            Output output = new Output();
            output.tables = tables;
            output.rawData = "";
            return output;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);

            return UIUtils.getErrorInfo(e);
        }

    }

}
