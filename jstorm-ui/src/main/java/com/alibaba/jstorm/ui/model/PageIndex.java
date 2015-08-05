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
package com.alibaba.jstorm.ui.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.ui.UIDef;
import com.alibaba.jstorm.utils.JStormUtils;

public class PageIndex implements Serializable {

    private static final long serialVersionUID = -6305581906533640556L;

    public String status;
    public LinkData linkData;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LinkData getLinkData() {
        return linkData;
    }

    public void setLinkData(LinkData linkData) {
        this.linkData = linkData;
    }
    
    public static class Event {
        public long totalSize;
        public long pos;
        public long pageSize;
        public String url;
        public Map<String, String> paramMap;
    }

    
    /**
     * @param index
     * @param pageSize
     */
    public static void insertPage(long index, String text,
            boolean isActive, 
            Event event, List<PageIndex> ret) {
        long pos = index * event.pageSize;

        PageIndex page = new PageIndex();

        LinkData linkData = new LinkData();
        page.linkData = linkData;

        linkData.setUrl(event.url);
        linkData.setText(text);
        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.putAll(event.paramMap);
        paramMap.put(UIDef.POS, String.valueOf(pos));
        linkData.setParamMap(paramMap);

//        if (isDisable == true) {
//            page.status = "disabled";
//        } else 
        if (isActive == true) {
            page.status = "active";
        }

        ret.add(page);
    }

    public static List<PageIndex> generatePageIndex(long totalSize,
    		long pageSize, String url,
    		Map<String, String> paramMap) {
    	PageIndex.Event event = new PageIndex.Event();
        event.totalSize = totalSize;
        event.pos = JStormUtils.parseLong(paramMap.get(UIDef.POS), 0);
        event.pageSize = pageSize;
        event.url = url;
        event.paramMap = paramMap;
        
        return generatePageIndex(event);
    }

    public static List<PageIndex> generatePageIndex(Event event) {
        List<PageIndex> ret = new ArrayList<PageIndex>();

        long pageNum = (event.totalSize + event.pageSize - 1) / event.pageSize;
        long currentPageIndex = event.pos/event.pageSize;

        if (pageNum <= 10) {
            for (long i = pageNum - 1; i >= 0; i--) {
                insertPage(i, String.valueOf(i), 
                        i == currentPageIndex, event, ret);
            }
            return ret;
        }

        if (pageNum - currentPageIndex < 5) {
            for (long i = pageNum - 1; i >= currentPageIndex; i--) {
                insertPage(i, String.valueOf(i), 
                        i == currentPageIndex, event, ret);
            }
        } else {
            insertPage(pageNum - 1, "End", 
                    pageNum - 1 == currentPageIndex, event, ret);
            insertPage(currentPageIndex + 4, "...",  false, event, ret);
            for (long i = currentPageIndex + 3; i >= currentPageIndex; i--) {
                insertPage(i, String.valueOf(i), 
                        i == currentPageIndex, event, ret);
            }
        }

        if (currentPageIndex < 5) {
            for (long i = currentPageIndex - 1; i > 0; i--) {
                insertPage(i, String.valueOf(i), 
                        i == currentPageIndex, event, ret);
            }
        } else {
            for (long i = currentPageIndex - 1; i >= currentPageIndex - 3; i--) {
                insertPage(i, String.valueOf(i), 
                        i == currentPageIndex, event, ret);
            }
            insertPage(currentPageIndex - 4, "...",  false, event, ret);
            insertPage(0, "Begin",  0 == currentPageIndex, event, ret);
        }
        
        return ret;
    }
}
