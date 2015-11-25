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
public class StatusTag extends SimpleTagSupport {

    public static final String ACTIVE = "active";
    public static final String INACTIVE = "inactive";
    public static final String STARTING = "starting";
    public static final String KILLED = "killed";
    public static final String REBALANCING = "rebalancing";
    private String status;

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public void doTag() throws JspException {
        JspWriter out = getJspContext().getOut();
        try {
            String label = "default";
            switch (status.toLowerCase()){
                case STARTING:
                    label = "primary";
                    break;
                case ACTIVE:
                    label = "success";
                    break;
                case KILLED:
                    label = "warning";
                    break;
                case INACTIVE:
                    label = "default";
                    break;
                case REBALANCING:
                    label = "info";
                    break;
            }
            String result = String.format("<span class='label label-%s'>%s</span>",label,status);
            out.write(result);
        } catch (IOException e) {
            throw new JspException("Error: " + e.getMessage());
        }
    }
}
