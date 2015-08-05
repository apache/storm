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
import javax.faces.context.FacesContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.ui.UIDef;
import com.alibaba.jstorm.ui.UIMetrics;
import com.alibaba.jstorm.ui.model.PageGenerator;
import com.alibaba.jstorm.ui.model.PageIndex;
import com.alibaba.jstorm.ui.model.TableData;

/**
 * 
 * @author xin.zhou/Longda
 */
@ManagedBean(name = "tablepage")
@ViewScoped
public class TablePage implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(TablePage.class);

    protected List<TableData> tables = new ArrayList<TableData>();
    private List<PageIndex> pages;
    protected Map<String, String> parameterMap;
    protected String rawData = "";
    
    public TablePage() throws Exception {
        FacesContext ctx = FacesContext.getCurrentInstance();
        parameterMap = ctx.getExternalContext().getRequestParameterMap();

        init();
    }

    public TablePage(Map<String, String> parameterMap) throws Exception {
        this.parameterMap = parameterMap;
        init();
    }

    public void init() {
        LOG.info(parameterMap.toString());
        String pageType = parameterMap.get(UIDef.PAGE_TYPE);
        if (pageType == null) {
            throw new IllegalArgumentException("Please set " + UIDef.PAGE_TYPE);
        }

        long start = System.nanoTime();
        PageGenerator pageGenerator = UIDef.pageGeneratos.get(pageType);
        if (pageGenerator == null) {
            throw new IllegalArgumentException("Invalid " + UIDef.PAGE_TYPE
                    + ":" + pageType);
        }
        
        try {
            PageGenerator.Output output = pageGenerator.generate(parameterMap);
            tables = output.tables;
            pages = output.pages;
            rawData = output.rawData;
        }finally {
            long end = System.nanoTime();
            UIMetrics.updateHistorgram(pageGenerator.getClass().getSimpleName(), (end - start)/1000000.0d);
        }
    }

    public List<TableData> getTables() {
        return tables;
    }

    public void addTable(TableData tableData) {
        tables.add(tableData);
    }

    public Map<String, String> getParameterMap() {
        return parameterMap;
    }

    public String getRawData() {
        return rawData;
    }

    public List<PageIndex> getPages() {
        return pages;
    }

    public void setPages(List<PageIndex> pages) {
        this.pages = pages;
    }

    public void generateTables() {

    }

    public static Map<String, String> testCluster() {
        Map<String, String> parameterMap = new HashMap<String, String>();
        parameterMap.put(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_CLUSTER);
        return parameterMap;
    }

    public static Map<String, String> testNimbusConfPage() {
        Map<String, String> parameterMap = new HashMap<String, String>();
        parameterMap.put(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_CONF);
        parameterMap.put(UIDef.CONF_TYPE, UIDef.CONF_TYPE_NIMBUS);
        return parameterMap;
    }

    public static Map<String, String> testListLogPage() {
        Map<String, String> parameterMap = new HashMap<String, String>();
        parameterMap.put(UIDef.PAGE_TYPE, UIDef.PAGE_TYPE_LISTLOG);
        parameterMap.put(UIDef.HOST, "free-56-156.shucang.alipay.net");
        parameterMap.put(UIDef.PORT, "7621");
        parameterMap.put(UIDef.DIR, ".");

        return parameterMap;
    }

    public static void main(String[] args) {
        // testCluster();
        Map<String, String> parameterMap = testCluster();
        try {
            TablePage table = new TablePage(parameterMap);

            System.out.println(table.getTables());
        } catch (Exception e) {
            // TODO Auto-generated catch block

        }
    }

}
