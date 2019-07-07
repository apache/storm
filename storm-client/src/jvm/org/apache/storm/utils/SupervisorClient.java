/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.utils;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.Supervisor;
import org.apache.storm.security.auth.ThriftClient;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for interacting with Supervisor server, now we use supervisor server mainly for cases below.
 * <ul>
 * <li>worker <- supervisor: get worker local assignment for a storm.</li>
 * <li>nimbus -> supervisor: assign assignments for a node.</li>
 * </ul>
 */
public class SupervisorClient extends ThriftClient implements SupervisorIfaceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SupervisorClient.class);
    private Supervisor.Client client;

    public SupervisorClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null, null);
    }

    public SupervisorClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, ThriftConnectionType.SUPERVISOR, host, port, timeout, null);
        client = new Supervisor.Client(protocol);
    }

    public SupervisorClient(Map conf, String host, Integer port, Integer timeout, String asUser) throws TTransportException {
        super(conf, ThriftConnectionType.SUPERVISOR, host, port, timeout, asUser);
        client = new Supervisor.Client(protocol);
    }

    public SupervisorClient(Map conf, String host) throws TTransportException {
        super(conf, ThriftConnectionType.SUPERVISOR, host, null, null, null);
        client = new Supervisor.Client(protocol);
    }

    public static SupervisorClient getConfiguredClient(Map conf, String host) {
        //use the default server port.
        int port = Integer.parseInt(conf.get(Config.SUPERVISOR_THRIFT_PORT).toString());
        return getConfiguredClientAs(conf, host, port, null);
    }

    public static SupervisorClient getConfiguredClient(Map conf, String host, int port) {
        return getConfiguredClientAs(conf, host, port, null);
    }

    public static SupervisorClient getConfiguredClientAs(Map conf, String host, int port, String asUser) {
        if (conf.containsKey(Config.STORM_DO_AS_USER)) {
            if (asUser != null && !asUser.isEmpty()) {
                LOG.warn("You have specified a doAsUser as param {} and a doAsParam as config, config will take precedence.",
                        asUser,
                        conf.get(Config.STORM_DO_AS_USER));
            }
            asUser = (String) conf.get(Config.STORM_DO_AS_USER);
        }
        try {
            return new SupervisorClient(conf, host, port, null, asUser);
        } catch (TTransportException e) {
            throw new RuntimeException("Failed to create a supervisor client for host " + host);
        }
    }

    @Override
    public Supervisor.Client getIface() {
        return client;
    }
}
