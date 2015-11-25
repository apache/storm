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

import java.util.List;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class ClusterConfig {
    private String zkRoot;
    private List<String> zkServers;
    private Integer zkPort;
    private Integer nimbusPort;         //nimbus log server port
    private Integer supervisorPort;     //supervisor log server port

    public ClusterConfig(String zkRoot, List<String> zkServers, Integer zkPort) {
        this.zkRoot = zkRoot;
        this.zkServers = zkServers;
        this.zkPort = zkPort;
    }

    public ClusterConfig(String zkRoot, List<String> zkServers, Integer zkPort, Integer nimbusPort, Integer supervisorPort) {
        this.zkRoot = zkRoot;
        this.zkServers = zkServers;
        this.zkPort = zkPort;
        this.nimbusPort = nimbusPort;
        this.supervisorPort = supervisorPort;
    }

    public boolean isAvailable(){
        if (zkRoot == null) return false;
        if (zkServers == null || zkServers.size() == 0) return false;
        if (zkPort == null) return false;
        return true;
    }

    public String getZkRoot() {
        return zkRoot;
    }

    public void setZkRoot(String zkRoot) {
        this.zkRoot = zkRoot;
    }

    public List<String> getZkServers() {
        return zkServers;
    }

    public void setZkServers(List<String> zkServers) {
        this.zkServers = zkServers;
    }

    public Integer getZkPort() {
        return zkPort;
    }

    public void setZkPort(Integer zkPort) {
        this.zkPort = zkPort;
    }

    public Integer getNimbusPort() {
        return nimbusPort;
    }

    public void setNimbusPort(Integer nimbusPort) {
        this.nimbusPort = nimbusPort;
    }

    public Integer getSupervisorPort() {
        return supervisorPort;
    }

    public void setSupervisorPort(Integer supervisorPort) {
        this.supervisorPort = supervisorPort;
    }

    @Override
    public String toString() {
        return "ClusterConfig{" +
                "zkRoot='" + zkRoot + '\'' +
                ", zkServers=" + zkServers +
                ", zkPort=" + zkPort +
                ", nimbusPort=" + nimbusPort +
                ", supervisorPort=" + supervisorPort +
                '}';
    }
}
