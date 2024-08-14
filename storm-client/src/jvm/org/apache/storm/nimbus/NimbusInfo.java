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
    private static final Pattern NON_TLS_HOST_PORT_PATTERN_FALLBACK = Pattern.compile("^(.*):([0-9]+)$");
    private static final Pattern TLS_HOST_PORT_PATTERN = Pattern.compile("^(.*):([0-9]+):([0-9]+)$");
    private String host;
    private int port;
    private int tlsPort;
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

    public NimbusInfo(String host, int port, int tlsPort, boolean isLeader) {
        this(host, port, isLeader);
        this.tlsPort = tlsPort;
    }

    public static NimbusInfo parse(String nimbusInfo) {
        Matcher m = TLS_HOST_PORT_PATTERN.matcher(nimbusInfo);
        if (m.matches()) {
            return new NimbusInfo(m.group(1), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)), false);
        } else {
            LOG.info("nimbusInfo {} doesn't match the format of host:port:tlsPort; fall back to the non-tls format host:port", nimbusInfo);
            m = NON_TLS_HOST_PORT_PATTERN_FALLBACK.matcher(nimbusInfo);
            if (m.matches()) {
                return new NimbusInfo(m.group(1), Integer.parseInt(m.group(2)), false);
            } else {
                throw new RuntimeException("nimbusInfo should have format of host:port:tlsPort or host:port, invalid string " + nimbusInfo);
            }
        }
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
            int tlsPort = ObjectReader.getInt(conf.get(Config.NIMBUS_THRIFT_TLS_PORT));
            return new NimbusInfo(host, port, tlsPort, false);

        } catch (UnknownHostException e) {
            throw new RuntimeException("Something wrong with network/dns config, host cant figure out its name", e);
        }
    }

    public String toHostPortString() {
        return String.format("%s:%s:%s", host, port, tlsPort);
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

    public int getTlsPort() {
        return tlsPort;
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

        if (tlsPort != that.tlsPort) {
            return false;
        }

        return host.equals(that.host);
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        result = 31 * result + tlsPort;
        result = 31 * result + (isLeader ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NimbusInfo{"
                + "host='" + host + '\''
                + ", port=" + port
                + ", tlsPort=" + tlsPort
                + ", isLeader=" + isLeader
                + '}';
    }
}
