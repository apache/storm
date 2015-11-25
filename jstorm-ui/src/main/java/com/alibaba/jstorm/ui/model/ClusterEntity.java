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

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class ClusterEntity {
    private String name;
    private int supervisor_num;
    private int topology_num;
    private String ip;
    private int port;   //logview port
    private String version;
    private int total_ports;
    private int used_ports;

    public ClusterEntity(String name, int supervisor_num, int topology_num, String ip, int port,
                         String version, int total_ports, int used_ports) {
        this.name = name;
        this.supervisor_num = supervisor_num;
        this.topology_num = topology_num;
        this.ip = ip;
        this.port = port;
        this.version = version;
        this.total_ports = total_ports;
        this.used_ports = used_ports;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSupervisor_num() {
        return supervisor_num;
    }

    public void setSupervisor_num(int supervisor_num) {
        this.supervisor_num = supervisor_num;
    }

    public int getTopology_num() {
        return topology_num;
    }

    public void setTopology_num(int topology_num) {
        this.topology_num = topology_num;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getTotal_ports() {
        return total_ports;
    }

    public void setTotal_ports(int total_ports) {
        this.total_ports = total_ports;
    }

    public int getUsed_ports() {
        return used_ports;
    }

    public void setUsed_ports(int used_ports) {
        this.used_ports = used_ports;
    }
}
