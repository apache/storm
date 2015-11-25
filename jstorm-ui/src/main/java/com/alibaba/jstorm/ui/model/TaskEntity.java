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

import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.TaskSummary;

import java.util.List;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class TaskEntity {
    private int task_id;
    private String component;
    private String type;
    private int uptime;
    private String status;
    private String host;
    private int port;
    private List<ErrorInfo> errors;

    public TaskEntity(int task_id, int uptime, String status, String host,
                      int port, List<ErrorInfo> errors) {
        this.task_id = task_id;
        this.uptime = uptime;
        this.status = status;
        this.host = host;
        this.port = port;
        this.errors = errors;
    }

    public TaskEntity(TaskSummary ts) {
        this(ts.get_taskId(), ts.get_uptime(), ts.get_status(), ts.get_host(), ts.get_port(), ts.get_errors());
    }

    public TaskEntity(int task_id, String component, String type) {
        this.task_id = task_id;
        this.component = component;
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getTask_id() {
        return task_id;
    }

    public void setTask_id(int task_id) {
        this.task_id = task_id;
    }

    public String getComponent() {
        return component;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public int getUptime() {
        return uptime;
    }

    public void setUptime(int uptime) {
        this.uptime = uptime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public List<ErrorInfo> getErrors() {
        return errors;
    }

    public void setErrors(List<ErrorInfo> errors) {
        this.errors = errors;
    }
}
