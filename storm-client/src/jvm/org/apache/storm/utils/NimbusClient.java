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

package org.apache.storm.utils;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.ThriftClient;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client used for connecting to nimbus.  Typically you want to use a variant of the
 * `getConfiguredClient` static method to get a client to use, as directly putting in
 * a host and port does not support nimbus high availability.
 */
public class NimbusClient extends ThriftClient {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusClient.class);
    private static volatile Nimbus.Iface _localOverrideClient = null;
    private static String oldLeader = "";
    /**
     * Indicates if this is a special client that is overwritten for local mode.
     */
    public final boolean isLocal;
    private final Nimbus.Iface client;

    public static class Builder {
        private Map<String, Object> conf;
        private String asUser;
        private Integer timeout;
        private boolean forDaemon = false;

        private Builder(Map<String, Object> conf) {
            this.conf = conf;
        }

        public static Builder withConf(Map<String, Object> conf) {
            return new Builder(conf);
        }

        public Builder asUser(String asUser) {
            this.asUser = asUser;
            return this;
        }

        public Builder withTimeout(Integer timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder forDaemon() {
            this.forDaemon = true;
            return this;
        }

        public NimbusClient buildWithNimbusHostPort(String host, Integer port) throws TTransportException {
            return new NimbusClient(conf, host, port, timeout, asUser, false);
        }

        public NimbusClient build()  {
            return createNimbusClient(conf, asUser, timeout);
        }
    }

    /**
     * Constructor.
     * @param conf the conf for the client.
     * @param host the host the client is to talk to.
     * @param port the port the client is to talk to.
     * @param timeout the timeout to use when connecting.
     * @throws TTransportException on any error.
     * @deprecated use {@link Builder #buildWithNimbusHostPort()} instead.
     */
    @Deprecated
    public NimbusClient(Map<String, Object> conf, String host, int port, Integer timeout) throws TTransportException {
        this(conf, host, port, timeout, null);
    }

    /**
     * Constructor.
     * @param conf the conf for the client.
     * @param host the host the client is to talk to.
     * @param port the port the client is to talk to.
     * @param timeout the timeout to use when connecting.
     * @param asUser the name of the user you want to impersonate (use with caution as it is not always supported).
     * @throws TTransportException on any error.
     * @deprecated use {@link Builder #buildWithNimbusHostPort()} instead.
     */
    @Deprecated
    public NimbusClient(Map<String, Object> conf, String host, Integer port, Integer timeout, String asUser) throws TTransportException {
        this(conf, host, port, timeout, asUser, false);
    }

    @Deprecated
    public NimbusClient(Map<String, Object> conf, String host, Integer port, Integer timeout, String asUser, boolean useTls)
            throws TTransportException {
        super(conf, useTls ? ThriftConnectionType.NIMBUS_TLS : ThriftConnectionType.NIMBUS, host, port, timeout, asUser);
        client = new Nimbus.Client(protocol);
        isLocal = false;
    }

    /**
     * Constructor.
     * @param conf the conf for the client.
     * @param host the host the client is to talk to.
     * @throws TTransportException on any error.
     * @deprecated use {@link Builder #buildWithNimbusHostPort()} instead.
     */
    @Deprecated
    public NimbusClient(Map<String, Object> conf, String host) throws TTransportException {
        this(conf, host, null, null, null);
    }

    private NimbusClient(Nimbus.Iface client) {
        super(new HashMap<>(), ThriftConnectionType.LOCAL_FAKE, "localhost", null, null, null);
        this.client = client;
        isLocal = true;
    }

    /**
     * Is the local override set or not.
     * @return true of new clients will be overridden to connect to a local cluster and not the configured remote cluster.
     */
    public static boolean isLocalOverride() {
        return _localOverrideClient != null;
    }

    /**
     * Execute cb with a configured nimbus client that will be closed once cb returns.
     * @param cb the callback to send to nimbus.
     * @throws Exception on any kind of error.
     */
    public static void withConfiguredClient(WithNimbus cb) throws Exception {
        withConfiguredClient(cb, ConfigUtils.readStormConfig());
    }

    /**
     * Execute cb with a configured nimbus client that will be closed once cb returns.
     * @param cb the callback to send to nimbus.
     * @param conf the conf to use instead of reading the global storm conf.
     * @throws Exception on any kind of error.
     */
    public static void withConfiguredClient(WithNimbus cb, Map<String, Object> conf) throws Exception {
        try (NimbusClient client = NimbusClient.Builder.withConf(conf).build()) {
            cb.run(client.getClient());
        }
    }

    /**
     * Get a nimbus client as configured by conf.
     * @param conf the configuration to use.
     * @return the client, don't forget to close it when done.
     * @deprecated use {@link Builder #build()} instead.
     */
    @Deprecated
    public static NimbusClient getConfiguredClient(Map<String, Object> conf) {
        return getConfiguredClientAs(conf, null);
    }

    /**
     * Get a nimbus client as configured by conf.
     * @param conf the configuration to use.
     * @param timeout the timeout to use when connecting.
     * @return the client, don't forget to close it when done.
     * @deprecated use {@link Builder #build()} instead.
     */
    @Deprecated
    public static NimbusClient getConfiguredClient(Map<String, Object> conf, Integer timeout) {
        return getConfiguredClientAs(conf, null, timeout);
    }

    /**
     * Check to see if we should log the leader we are connecting to or not.  This typically happens when the leader changes or if debug
     * logging is enabled. The code remembers the last leader it was called with, but it should be transparent to the caller.
     *
     * @param leader the leader we are trying to connect to.
     * @return true if it should be logged else false.
     */
    private static synchronized boolean shouldLogLeader(String leader) {
        assert leader != null;
        if (LOG.isDebugEnabled()) {
            //If debug logging is turned on we should just log the leader all the time....
            return true;
        }
        //Only log if the leader has changed.  It is not interesting otherwise.
        if (oldLeader.equals(leader)) {
            return false;
        }
        oldLeader = leader;
        return true;
    }

    /**
     * Get a nimbus client as configured by conf.
     * @param conf the configuration to use.
     * @param asUser the user to impersonate (this does not always work).
     * @return the client, don't forget to close it when done.
     * @deprecated use {@link Builder #build()} instead.
     */
    @Deprecated
    public static NimbusClient getConfiguredClientAs(Map<String, Object> conf, String asUser) {
        return getConfiguredClientAs(conf, asUser, null);
    }

    /**
     * Get a nimbus client as configured by conf.
     * @param conf the configuration to use.
     * @param asUser the user to impersonate (this does not always work).
     * @param timeout the timeout to use when connecting.
     * @return the client, don't forget to close it when done.
     * @deprecated use {@link Builder #build()} instead.
     */
    @Deprecated
    public static NimbusClient getConfiguredClientAs(Map<String, Object> conf, String asUser, Integer timeout) {
        return createNimbusClient(conf, asUser, timeout);
    }

    private static NimbusClient createNimbusClient(Map<String, Object> conf, String asUser, Integer timeout) {
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
                LOG.warn("You have specified a doAsUser as param {} and a doAsParam as config, config will take precedence.",
                         asUser, conf.get(Config.STORM_DO_AS_USER));
            }
            asUser = (String) conf.get(Config.STORM_DO_AS_USER);
        }

        if (asUser == null || asUser.isEmpty()) {
            //The user is not set so lets see what the request context is.
            ReqContext context = ReqContext.context();
            Principal principal = context.principal();
            asUser = principal == null ? null : principal.getName();
            LOG.debug("Will impersonate {} based off of request context.", asUser);
        }

        List<String> seeds = (List<String>) conf.get(Config.NIMBUS_SEEDS);

        boolean useTls = ObjectReader.getBoolean(conf.get(Config.NIMBUS_THRIFT_CLIENT_USE_TLS), false);
        if (useTls && null == ObjectReader.getString(conf.get(Config.NIMBUS_THRIFT_TLS_TRANSPORT_PLUGIN))) {
            throw new RuntimeException(Config.NIMBUS_THRIFT_TLS_TRANSPORT_PLUGIN
                    + " must be set to use a transport plugin that supports tls");
        }

        int port = Integer.parseInt(conf.get(Config.NIMBUS_THRIFT_PORT).toString());
        int tlsPort = Integer.parseInt(conf.get(Config.NIMBUS_THRIFT_TLS_PORT).toString());
        int configuredPortToUse = useTls ? tlsPort : port;

        for (String host : seeds) {
            NimbusSummary nimbusSummary;
            NimbusClient client = null;
            try {
                client = new NimbusClient(conf, host, configuredPortToUse, timeout, asUser, useTls);
                nimbusSummary = client.getClient().getLeader();
                if (nimbusSummary != null) {
                    String leaderNimbus = nimbusSummary.get_host() + ":" + nimbusSummary.get_port() + ":" + nimbusSummary.get_tlsPort();
                    if (shouldLogLeader(leaderNimbus)) {
                        LOG.info("Found leader nimbus : {}", leaderNimbus);
                    }

                    int nimbusPortFromSummary = useTls ? nimbusSummary.get_tlsPort() : nimbusSummary.get_port();
                    if (nimbusSummary.get_host().equals(host) && nimbusPortFromSummary == port) {
                        NimbusClient ret = client;
                        client = null;
                        return ret;
                    }
                    try {
                        return new NimbusClient(conf, nimbusSummary.get_host(), nimbusPortFromSummary, timeout, asUser, useTls);
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
            throw new NimbusLeaderNotFoundException("Could not find a nimbus leader, please try again after some time.");
        }
        throw new NimbusLeaderNotFoundException(
            "Could not find leader nimbus from seed hosts " + seeds + ". "
            + "Did you specify a valid list of nimbus hosts for config "
            + Config.NIMBUS_SEEDS + "?");
    }

    /**
     * Get the underlying thrift client.
     * @return the underlying thrift client.
     */
    public Nimbus.Iface getClient() {
        return client;
    }

    /**
     * An interface to allow callbacks with a thrift nimbus client.
     */
    public interface WithNimbus {
        /**
         * Run what you need with the nimbus client.
         * @param client the client.
         * @throws Exception on any error.
         */
        void run(Nimbus.Iface client) throws Exception;
    }

    public static final class LocalOverride implements AutoCloseable {
        public LocalOverride(Nimbus.Iface client) {
            _localOverrideClient = client;
        }

        @Override
        public void close() throws Exception {
            _localOverrideClient = null;
        }
    }
}
