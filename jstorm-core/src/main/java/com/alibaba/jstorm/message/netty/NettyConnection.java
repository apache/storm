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
package com.alibaba.jstorm.message.netty;

import java.io.Serializable;

import com.alibaba.jstorm.utils.NetWorkUtils;

public class NettyConnection implements Serializable {
    protected String clientPort;
    protected String serverPort;

    public String getClientPort() {
        return clientPort;
    }

    public void setClientPort(String client, int port) {
        String ip = NetWorkUtils.host2Ip(client);
        clientPort = ip + ":" + port;
    }

    public String getServerPort() {
        return serverPort;
    }

    public void setServerPort(String server, int port) {
        String ip = NetWorkUtils.host2Ip(server);
        serverPort = ip + ":" + port;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clientPort == null) ? 0 : clientPort.hashCode());
        result = prime * result + ((serverPort == null) ? 0 : serverPort.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        NettyConnection other = (NettyConnection) obj;
        if (clientPort == null) {
            if (other.clientPort != null)
                return false;
        } else if (!clientPort.equals(other.clientPort))
            return false;
        if (serverPort == null) {
            if (other.serverPort != null)
                return false;
        } else if (!serverPort.equals(other.serverPort))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return clientPort + "->" + serverPort;
    }

    public static String mkString(String client, int clientPort, String server, int serverPort) {
        return client + ":" + clientPort + "->" + server + ":" + serverPort;
    }

}
