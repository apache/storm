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

import com.alibaba.jstorm.common.metric.old.window.StatBuckets;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class NimbusEntity {
    private String host;
    private String ip;
    private String port;
    private String uptime_secs;
    private int supervisor_num;
    private int total_port_num;
    private int used_port_num;
    private int free_port_num;
    private int task_num;
    private int topology_num;
    private String version;


    public NimbusEntity(String host, String uptime_secs, int supervisor_num, int total_port_num,
                        int used_port_num, int free_port_num, int task_num, int topology_num, String version) {
        this.host = host;
        this.ip = UIUtils.getIp(host);
        this.port = UIUtils.getPort(host);
        this.uptime_secs = StatBuckets.prettyUptime(JStormUtils.parseInt(uptime_secs, 0));
        this.supervisor_num = supervisor_num;
        this.total_port_num = total_port_num;
        this.used_port_num = used_port_num;
        this.free_port_num = free_port_num;
        this.task_num = task_num;
        this.topology_num = topology_num;
        this.version = version;
    }

    public NimbusEntity(String host, String uptime_secs, String version) {
        this.host = host;
        this.ip = UIUtils.getIp(host);
        this.port = UIUtils.getPort(host);
        this.uptime_secs = StatBuckets.prettyUptime(JStormUtils.parseInt(uptime_secs, 0));
        this.version = version;
    }


    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUptime_secs() {
        return uptime_secs;
    }

    public void setUptime_secs(String uptime_secs) {
        this.uptime_secs = uptime_secs;
    }

    public int getSupervisor_num() {
        return supervisor_num;
    }

    public void setSupervisor_num(int supervisor_num) {
        this.supervisor_num = supervisor_num;
    }

    public int getTotal_port_num() {
        return total_port_num;
    }

    public void setTotal_port_num(int total_port_num) {
        this.total_port_num = total_port_num;
    }

    public int getUsed_port_num() {
        return used_port_num;
    }

    public void setUsed_port_num(int used_port_num) {
        this.used_port_num = used_port_num;
    }

    public int getFree_port_num() {
        return free_port_num;
    }

    public void setFree_port_num(int free_port_num) {
        this.free_port_num = free_port_num;
    }

    public int getTask_num() {
        return task_num;
    }

    public void setTask_num(int task_num) {
        this.task_num = task_num;
    }

    public int getTopology_num() {
        return topology_num;
    }

    public void setTopology_num(int topology_num) {
        this.topology_num = topology_num;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
