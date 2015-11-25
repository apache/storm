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
package com.alibaba.jstorm.ui.tags;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.JspWriter;
import javax.servlet.jsp.tagext.SimpleTagSupport;
import java.io.IOException;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class PaginationTag extends SimpleTagSupport {

    private int pageSize;
    private int curPage;
    private String urlBase;

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public void setCurPage(int curPage) {
        this.curPage = curPage;
    }

    public void setUrlBase(String urlBase) {
        this.urlBase = urlBase;
    }

    @Override
    public void doTag() throws JspException {
        if (pageSize <= 1)  return;
        JspWriter out = getJspContext().getOut();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("<nav class='netty-pagination'><ul class='pagination'>");
            if (curPage > 1) {
                sb.append(String.format("<li><a href='%s' class='previous'>", getUrl(curPage - 1)));
                sb.append("<span class='glyphicon glyphicon-chevron-left'></span></a>");
                sb.append("</li>");
            }
            if (pageSize <= 10) {
                for (int i = 1; i <= pageSize; i++) {
                    sb.append(createPage(i, i == curPage));
                }
            } else {
                if (curPage <= 5) {
                    for (int i = 1; i <= curPage; i++) {
                        sb.append(createPage(i, i == curPage));
                    }
                } else {
                    sb.append(createPage(curPage - 4, "...", false));
                    for (int i = curPage - 3; i <= curPage; i++) {
                        sb.append(createPage(i, i == curPage));
                    }
                }

                if (pageSize - curPage <= 5) {
                    for (int i = curPage + 1; i <= pageSize; i++) {
                        sb.append(createPage(i, false));
                    }
                }else{
                    for (int i = curPage + 1; i <= curPage + 3; i++) {
                        sb.append(createPage(i, false));
                    }
                    sb.append(createPage(curPage + 4, "...", false));
                }
            }

            if (curPage < pageSize) {
                sb.append(String.format("<li><a href='%s' class='next'>", getUrl(curPage + 1)));
                sb.append("<span class='glyphicon glyphicon-chevron-right'></span></a>");
                sb.append("</li>");
            }


            sb.append("</ul></nav>");
            out.write(sb.toString());
        } catch (IOException e) {
            throw new JspException("Error: " + e.getMessage());
        }
    }

    private String createPage(int page, String text, boolean isActive) {
        if (isActive) {
            return String.format("<li class='%s'><a href='%s'>%s</a></li>", "active", getUrl(page), text);
        }else{
            return String.format("<li><a href='%s'>%s</a></li>", getUrl(page), text);
        }
    }

    private String createPage(int page, boolean isActive) {
        return createPage(page, page + "", isActive);
    }

    private String getUrl(int page) {
        return urlBase + "&page=" + page;
    }
}
