/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.nimbus;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.storm.Config;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NimbusInfo implements Serializable {
    private static final long serialVersionUID = 2161446155116099333L;
    private static final Logger LOG = LoggerFactory.getLogger(NimbusInfo.class);
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("^(.*):([0-9]+)$");
    private String host;
    private int port;
    private boolean isLeader;

    public NimbusInfo(String host, int port, boolean isLeader) {
        if (host == null) {
            throw new NullPointerException("Host cannot be null");
        }
        if (port < 0) {
            throw new IllegalArgumentException("Port cannot be negative");
        }
        this.host = host;
        this.port = port;
        this.isLeader = isLeader;
    }

    public static NimbusInfo parse(String nimbusInfo) {
        Matcher m = HOST_PORT_PATTERN.matcher(nimbusInfo);
        if (!m.matches()) {
            throw new RuntimeException("nimbusInfo should have format of host:port, invalid string " + nimbusInfo);
        }
        return new NimbusInfo(m.group(1), Integer.valueOf(m.group(2)), false);
    }

    public static NimbusInfo fromConf(Map<String, Object> conf) {
        try {
            String host = InetAddress.getLocalHost().getCanonicalHostName();
            if (conf.containsKey(Config.STORM_LOCAL_HOSTNAME)) {
                host = (String) conf.get(Config.STORM_LOCAL_HOSTNAME);
                LOG.info("Overriding nimbus host to storm.local.hostname -> {}", host);
            } else {
                LOG.info("Nimbus figures out its name to {}", host);
            }

            int port = ObjectReader.getInt(conf.get(Config.NIMBUS_THRIFT_PORT), 6627);
            return new NimbusInfo(host, port, false);

        } catch (UnknownHostException e) {
            throw new RuntimeException("Something wrong with network/dns config, host cant figure out its name", e);
        }
    }

    public String toHostPortString() {
        return String.format("%s:%s", host, port);
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NimbusInfo)) {
            return false;
        }

        NimbusInfo that = (NimbusInfo) o;

        if (isLeader != that.isLeader) {
            return false;
        }
        if (port != that.port) {
            return false;
        }
        return host.equals(that.host);
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        result = 31 * result + (isLeader ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NimbusInfo{"
                + "host='" + host + '\''
                + ", port=" + port
                + ", isLeader=" + isLeader
                + '}';
    }
}
