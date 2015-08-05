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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.ui.UIDef;
import com.alibaba.jstorm.ui.UIMetrics;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ColumnData;
import com.alibaba.jstorm.ui.model.LinkData;
import com.alibaba.jstorm.ui.model.TableData;

/**
 * 
 * @author xin.zhou/Longda
 */
@ManagedBean(name = "homepage")
@ViewScoped
public class HomePage implements Serializable {

    private static final long serialVersionUID = -6103468603521876731L;

    private static final Logger LOG = LoggerFactory.getLogger(HomePage.class);

    public static final String SINGLE_CLUSTER = "single";
    public static final String MULTI_CLUSTER = "multi";

    public static final String HEADER_ZK_ROOT = "ZK Root";
    public static final String HEADER_ZK_SERVERS = "ZK Servers";
    public static final String HEADER_ZK_PORT = "ZK Port";

    protected String clusterType;
    protected List<TableData> tables = new ArrayList<TableData>();
    protected Map<String, String> parameterMap;
    
    static {
        // add code to load UIMetrics
        UIMetrics.mkInstance();
    }

    public HomePage() throws Exception {
        // FacesContext ctx = FacesContext.getCurrentInstance();
        // parameterMap = ctx.getExternalContext().getRequestParameterMap();

        init();
    }

    @SuppressWarnings("rawtypes")
    public void init() {
        generateTables();

        if (tables == null || tables.size() == 0
                || tables.get(0).getLines().size() == 1) {
            clusterType = SINGLE_CLUSTER;
        } else {
            clusterType = MULTI_CLUSTER;
        }
    }

    public void generateTables() {
        long start = System.nanoTime();
        try {
            LOG.info("ClusterPage init...");
            Map conf = UIUtils.readUiConfig();
            List<Map> uiClusters = ConfigExtension.getUiClusters(conf);
            if (uiClusters == null) {
                return;
            }

            TableData table = new TableData();
            tables.add(table);

            List<String> headers = table.getHeaders();
            List<Map<String, ColumnData>> lines = table.getLines();
            table.setName("Cluster List");

            headers.add(StringUtils.capitalize(UIDef.CLUSTER));
            headers.add(HEADER_ZK_ROOT);
            headers.add(HEADER_ZK_SERVERS);
            headers.add(HEADER_ZK_PORT);

            for (Map cluster : uiClusters) {
                Map<String, ColumnData> line =
                        new HashMap<String, ColumnData>();
                lines.add(line);

                String clusterName = ConfigExtension.getUiClusterName(cluster);
                ColumnData clusterColumn = new ColumnData();
                LinkData linkData = new LinkData();
                linkData.setUrl(UIDef.LINK_TABLE_PAGE);
                linkData.setText(clusterName);
                linkData.addParam(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_CLUSTER);
                linkData.addParam(UIDef.CLUSTER, clusterName);

                clusterColumn.addLinkData(linkData);

                line.put(StringUtils.capitalize(UIDef.CLUSTER), clusterColumn);

                String zkRoot = ConfigExtension.getUiClusterZkRoot(cluster);
                ColumnData zkRootColumn = new ColumnData();
                zkRootColumn.addText(zkRoot);
                line.put(HEADER_ZK_ROOT, zkRootColumn);

                List<String> servers =
                        ConfigExtension.getUiClusterZkServers(cluster);
                ColumnData zkServerColumn = new ColumnData();
                for (String server : servers) {
                    zkServerColumn.addText(server);
                }
                line.put(HEADER_ZK_SERVERS, zkServerColumn);

                String port =
                        String.valueOf(ConfigExtension
                                .getUiClusterZkPort(cluster));
                ColumnData zkPortColumn = new ColumnData();
                zkPortColumn.addText(port);
                line.put(HEADER_ZK_PORT, zkPortColumn);
            }

        } catch (Exception e) {
            LOG.error("Failed to get cluster information:", e);
            throw new RuntimeException(e);
        } finally {
            long end = System.nanoTime();
            UIMetrics.updateHistorgram(this.getClass().getSimpleName(), (end - start)/1000000.0d);
        
            LOG.info("Finish ClusterPage");
        }
    }

    public String getClusterType() {
        return clusterType;
    }

    public List<TableData> getTables() {
        return tables;
    }

    public Map<String, String> getParameterMap() {
        return parameterMap;
    }

    public static void main(String[] args) {
        try {
            HomePage c = new HomePage();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
