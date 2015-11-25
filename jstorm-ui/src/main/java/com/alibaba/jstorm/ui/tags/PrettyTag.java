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

import com.alibaba.jstorm.common.metric.old.window.StatBuckets;
import com.alibaba.jstorm.ui.utils.UIDef;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.JspWriter;
import javax.servlet.jsp.tagext.SimpleTagSupport;
import java.io.IOException;
import java.text.DecimalFormat;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class PrettyTag extends SimpleTagSupport {

    private String input;
    private String type;


    public void setInput(String input) {
        this.input = input;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public void doTag() throws JspException {
        JspWriter out = getJspContext().getOut();
        try {
            String output;
            switch (type) {
                case "uptime":
                    int uptime = JStormUtils.parseInt(input, 0);
                    output = StatBuckets.prettyUptime(uptime);
                    break;
                case "datetime":
                    output = UIUtils.prettyDateTime(input);
                    break;
                case "filesize":
                    long size = UIUtils.parseLong(input, 0);
                    output = UIUtils.prettyFileSize(size);
                    break;
                case "head":
                    output = prettyHead(input);
                    break;
                default:
                    output = "Input Error";
                    break;
            }

            out.write(output);
        } catch (IOException e) {
            throw new JspException("Error: " + e.getMessage());
        }
    }

    private String prettyHead(String head) {
        if (UIDef.HEAD_MAP.containsKey(head)) {
            return UIDef.HEAD_MAP.get(head);
        } else {
            return head;
        }
    }


}
