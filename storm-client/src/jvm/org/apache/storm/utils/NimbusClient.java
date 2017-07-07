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
package org.apache.storm.utils;

import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.ThriftClient;
import org.apache.storm.security.auth.ThriftConnectionType;
import com.google.common.collect.Lists;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NimbusClient extends ThriftClient {
    private static volatile Nimbus.Iface _localOverrideClient = null;

    public static final class LocalOverride implements AutoCloseable {
        public LocalOverride(Nimbus.Iface client) {
            _localOverrideClient = client;
        }
        
        @Override
        public void close() throws Exception {
            _localOverrideClient = null;
        }
    }
    
    /**
     * @return true of new clients will be overridden to connect to a local cluster
     * and not the configured remote cluster.
     */
    public static boolean isLocalOverride() {
        return _localOverrideClient != null;
    }
    
    private Nimbus.Iface _client;
    /**
     * Indicates if this is a special client that is overwritten for local mode.
     */
    public final boolean _isLocal;
    private static final Logger LOG = LoggerFactory.getLogger(NimbusClient.class);

    public interface WithNimbus {
        public void run(Nimbus.Iface client) throws Exception;
    }

    public static void withConfiguredClient(WithNimbus cb) throws Exception {
        withConfiguredClient(cb, ConfigUtils.readStormConfig());
    }

    public static void withConfiguredClient(WithNimbus cb, Map<String, Object> conf) throws Exception {
        ReqContext context = ReqContext.context();
        Principal principal = context.principal();
        String user = principal == null ? null : principal.getName();
        try (NimbusClient client = getConfiguredClientAs(conf, user);) {
            cb.run(client.getClient());
        }
    }

    public static NimbusClient getConfiguredClient(Map<String, Object> conf) {
        return getConfiguredClientAs(conf, null);
    }
    
    public static NimbusClient getConfiguredClient(Map<String, Object> conf, Integer timeout) {
        return getConfiguredClientAs(conf, null, timeout);
    }
    
    public static NimbusClient getConfiguredClientAs(Map conf, String asUser) {
        return getConfiguredClientAs(conf, asUser, null);
    }

    public static NimbusClient getConfiguredClientAs(Map<String, Object> conf, String asUser, Integer timeout) {
        Nimbus.Iface override = _localOverrideClient;
        if (override != null) {
            return new NimbusClient(override);
        }
        Map<String, Object> fullConf = Utils.readStormConfig();
        fullConf.putAll(Utils.readCommandLineOpts());
        fullConf.putAll(conf);
        conf = fullConf;
        if (conf.containsKey(Config.STORM_DO_AS_USER)) {
            if (asUser != null && !asUser.isEmpty()) {
                LOG.warn("You have specified a doAsUser as param {} and a doAsParam as config, config will take precedence."
                        , asUser, conf.get(Config.STORM_DO_AS_USER));
            }
            asUser = (String) conf.get(Config.STORM_DO_AS_USER);
        }

        List<String> seeds;
        if(conf.containsKey(Config.NIMBUS_HOST)) {
            LOG.warn("Using deprecated config {} for backward compatibility. Please update your storm.yaml so it only has config {}",
                     Config.NIMBUS_HOST, Config.NIMBUS_SEEDS);
            seeds = Lists.newArrayList(conf.get(Config.NIMBUS_HOST).toString());
        } else {
            seeds = (List<String>) conf.get(Config.NIMBUS_SEEDS);
        }

        for (String host : seeds) {
            int port = Integer.parseInt(conf.get(Config.NIMBUS_THRIFT_PORT).toString());
            NimbusSummary nimbusSummary;
            NimbusClient client = null;
            try {
                client = new NimbusClient(conf, host, port, timeout, asUser);
                nimbusSummary = client.getClient().getLeader();
                if (nimbusSummary != null) {
                    String leaderNimbus = nimbusSummary.get_host() + ":" + nimbusSummary.get_port();
                    LOG.info("Found leader nimbus : {}", leaderNimbus);
                    if (nimbusSummary.get_host().equals(host) && nimbusSummary.get_port() == port) {
                        NimbusClient ret = client;
                        client = null;
                        return ret;
                    }
                    try {
                        return new NimbusClient(conf, nimbusSummary.get_host(), nimbusSummary.get_port(), timeout, asUser);
                    } catch (TTransportException e) {
                        throw new RuntimeException("Failed to create a nimbus client for the leader " + leaderNimbus, e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Ignoring exception while trying to get leader nimbus info from " + host
                                 + ". will retry with a different seed host.", e);
                continue;
            } finally {
                if (client != null) {
                    client.close();
                }
            }
            throw new NimbusLeaderNotFoundException("Could not find a nimbus leader, please try " +
                                                            "again after some time.");
        }
        throw new NimbusLeaderNotFoundException(
                "Could not find leader nimbus from seed hosts " + seeds + ". " +
                        "Did you specify a valid list of nimbus hosts for config " +
                        Config.NIMBUS_SEEDS + "?");
    }

    public NimbusClient(Map<String, Object> conf, String host, int port) throws TTransportException {
        this(conf, host, port, null, null);
    }

    public NimbusClient(Map<String, Object> conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, ThriftConnectionType.NIMBUS, host, port, timeout, null);
        _client = new Nimbus.Client(_protocol);
        _isLocal = false;
    }

    public NimbusClient(Map<String, Object> conf, String host, Integer port, Integer timeout, String asUser) throws TTransportException {
        super(conf, ThriftConnectionType.NIMBUS, host, port, timeout, asUser);
        _client = new Nimbus.Client(_protocol);
        _isLocal = false;
    }

    public NimbusClient(Map<String, Object> conf, String host) throws TTransportException {
        super(conf, ThriftConnectionType.NIMBUS, host, null, null, null);
        _client = new Nimbus.Client(_protocol);
        _isLocal = false;
    }
    
    private NimbusClient(Nimbus.Iface client) {
        super(new HashMap<>(), ThriftConnectionType.LOCAL_FAKE, "localhost", null, null, null);
        _client = client;
        _isLocal = true;
    }

    public Nimbus.Iface getClient() {
        return _client;
    }
}
