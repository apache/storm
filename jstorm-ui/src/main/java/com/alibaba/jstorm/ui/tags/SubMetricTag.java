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
import java.util.Map;
import java.util.Set;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class SubMetricTag extends SimpleTagSupport {


    private Map<String, String> metric;
    private Set<String> parentComp;
    private String metricName;
    private boolean isHidden = true;
    private String clazz = "sub-comp";

    public void setMetric(Map<String, String> metric) {
        this.metric = metric;
    }

    public void setParentComp(Set<String> parentComp) {
        this.parentComp = parentComp;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public void setIsHidden(boolean isHidden) {
        this.isHidden = isHidden;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    @Override
    public void doTag() throws JspException {
        JspWriter out = getJspContext().getOut();
        try {
            StringBuilder sb = new StringBuilder();
            if (metric.size() > 0) {
                if (isHidden) {
                    sb.append(String.format("<div class='%s hidden'>", clazz));
                } else {
                    sb.append(String.format("<div class='%s'>", clazz));
                }
                for (String parent : parentComp) {
                    String key = metricName + "@" + parent;
                    String v = metric.get(key);
                    if (v != null) {
                        sb.append(v);
                    }
                    sb.append("<br/>");
                }
                sb.append("</div>");
                out.write(sb.toString());
            }
        } catch (IOException e) {
            throw new JspException("Error: " + e.getMessage());
        }
    }


}
