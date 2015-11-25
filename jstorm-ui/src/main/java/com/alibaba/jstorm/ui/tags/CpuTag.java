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

import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.lang.StringUtils;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.JspWriter;
import javax.servlet.jsp.tagext.SimpleTagSupport;
import java.io.IOException;
import java.text.DecimalFormat;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class CpuTag extends SimpleTagSupport {

    private static final double THRESHOLD = 0.8;
    private static final DecimalFormat format = new DecimalFormat(".##");

    private String ratio;
    private int workers;

    public void setRatio(String ratio) {
        this.ratio = ratio;
    }

    public void setWorkers(int workers) {
        this.workers = workers;
    }

    @Override
    public void doTag() throws JspException {
        JspWriter out = getJspContext().getOut();
        try {
            if (!StringUtils.isBlank(ratio)) {
                double value = JStormUtils.parseDouble(ratio);
                double danger_threshold = workers * 100;
                double warn_threshold = danger_threshold * THRESHOLD;
                String status_success = "success";
                String status_warning = "warning";
                String status_danger = "danger";


                StringBuilder sb = new StringBuilder();
                sb.append("<div class='progress cpu-ratio-bar'>");
                if (Double.compare(value, warn_threshold) <= 0) {
                    double width = value / danger_threshold * 100;
                    appendBar(sb, width, value, status_success);
                } else if (Double.compare(value, danger_threshold) <= 0) {
                    double width1 = THRESHOLD * 100;
                    double width2 = (value - warn_threshold) / danger_threshold * 100;
                    appendBar(sb, width1, value, status_success);
                    appendBar(sb, width2, value, status_warning);
                } else{
                    double width1 = danger_threshold / value * 100;
                    double width2 = 100 - width1;
                    appendBar(sb, width1, value, status_success);
                    appendBar(sb, width2, value, status_danger);
                }
                sb.append("</div>");
                out.write(sb.toString());
            }

        } catch (IOException e) {
            throw new JspException("Error: " + e.getMessage());
        }
    }

    private String format(double v) {
        return format.format(v);
    }

    private void appendBar(StringBuilder sb, double width, double value, String status) {
        sb.append(String.format("<div class='progress-bar progress-bar-%s' style='min-width: 2em;width: %s%%'>", status, width));
        if (status.equals("success")) {
            sb.append(String.format("<span>%s%%</span>", format(value)));
        }
        sb.append("</div>");
    }
}
