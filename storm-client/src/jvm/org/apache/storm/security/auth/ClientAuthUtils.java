/*
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

package org.apache.storm.security.auth;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.security.URIParameter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import org.apache.storm.Config;
import org.apache.storm.generated.WorkerToken;
import org.apache.storm.generated.WorkerTokenInfo;
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.shade.org.apache.commons.codec.binary.Hex;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientAuthUtils {
    public static final String LOGIN_CONTEXT_SERVER = "StormServer";
    public static final String LOGIN_CONTEXT_CLIENT = "StormClient";
    public static final String LOGIN_CONTEXT_PACEMAKER_DIGEST = "PacemakerDigest";
    public static final String LOGIN_CONTEXT_PACEMAKER_SERVER = "PacemakerServer";
    public static final String LOGIN_CONTEXT_PACEMAKER_CLIENT = "PacemakerClient";
    public static final String SERVICE = "storm_thrift_server";
    private static final Logger LOG = LoggerFactory.getLogger(ClientAuthUtils.class);
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    public static String getJaasConf(Map<String, Object> topoConf) {
        return (String) topoConf.get("java.security.auth.login.config");
    }

    /**
     * Construct a JAAS configuration object per storm configuration file.
     *
     * @param topoConf Storm configuration
     * @return JAAS configuration object
     */
    public static Configuration getConfiguration(Map<String, Object> topoConf) {
        Configuration loginConf = null;

        //find login file configuration from Storm configuration
        String loginConfigurationFile = getJaasConf(topoConf);
        if ((loginConfigurationFile != null) && (loginConfigurationFile.length() > 0)) {
            File configFile = new File(loginConfigurationFile);
            if (!configFile.canRead()) {
                throw new RuntimeException("File " + loginConfigurationFile + " cannot be read.");
            }
            try {
                URI configUri = configFile.toURI();
                loginConf = Configuration.getInstance("JavaLoginConfig", new URIParameter(configUri));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        return loginConf;
    }

    /**
     * Get configurations for a section.
     *
     * @param configuration The config to pull the key/value pairs out of.
     * @param section       The app configuration entry name to get stuff from.
     * @return Return array of config entries or null if configuration is null
     */
    public static AppConfigurationEntry[] getEntries(Configuration configuration,
                                                     String section) throws IOException {
        if (configuration == null) {
            return null;
        }

        AppConfigurationEntry[] configurationEntries = configuration.getAppConfigurationEntry(section);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '" + section + "' entry in this configuration.";
            throw new IOException(errorMessage);
        }
        return configurationEntries;
    }

    /**
     * Pull a set of keys out of a Configuration.
     *
     * @param topoConf  The config containing the jaas conf file.
     * @param section       The app configuration entry name to get stuff from.
     * @return Return a map of the configs in conf.
     */
    public static SortedMap<String, ?> pullConfig(Map<String, Object> topoConf,
                                                  String section) throws IOException {
        Configuration configuration = ClientAuthUtils.getConfiguration(topoConf);
        AppConfigurationEntry[] configurationEntries = ClientAuthUtils.getEntries(configuration, section);

        if (configurationEntries == null) {
            return null;
        }

        TreeMap<String, Object> results = new TreeMap<>();

        for (AppConfigurationEntry entry : configurationEntries) {
            Map<String, ?> options = entry.getOptions();
            for (String key : options.keySet()) {
                results.put(key, options.get(key));
            }
        }

        return results;
    }

    /**
     * Pull a the value given section and key from Configuration.
     *
     * @param topoConf   The config containing the jaas conf file.
     * @param section       The app configuration entry name to get stuff from.
     * @param key           The key to look up inside of the section
     * @return Return a the String value of the configuration value
     */
    public static String get(Map<String, Object> topoConf, String section, String key) throws IOException {
        Configuration configuration = ClientAuthUtils.getConfiguration(topoConf);
        return get(configuration, section, key);
    }

    static String get(Configuration configuration, String section, String key) throws IOException {
        AppConfigurationEntry[] configurationEntries = ClientAuthUtils.getEntries(configuration, section);

        if (configurationEntries == null) {
            return null;
        }

        for (AppConfigurationEntry entry : configurationEntries) {
            Object val = entry.getOptions().get(key);
            if (val != null) {
                return (String) val;
            }
        }
        return null;
    }

    /**
     * Construct a principal to local plugin.
     *
     * @param topoConf storm configuration
     * @return the plugin
     */
    public static IPrincipalToLocal getPrincipalToLocalPlugin(Map<String, Object> topoConf) {
        IPrincipalToLocal ptol = null;
        try {
            String ptolClassname = (String) topoConf.get(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN);
            if (ptolClassname == null) {
                LOG.warn("No principal to local given {}", Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN);
            } else {
                ptol = ReflectionUtils.newInstance(ptolClassname);
                //TODO this can only ever be null if someone is doing something odd with mocking
                // We should really fix the mocking and remove this
                if (ptol != null) {
                    ptol.prepare(topoConf);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ptol;
    }

    /**
     * Construct a group mapping service provider plugin.
     *
     * @param conf daemon configuration
     * @return the plugin
     */
    public static IGroupMappingServiceProvider getGroupMappingServiceProviderPlugin(Map<String, Object> conf) {
        IGroupMappingServiceProvider gmsp = null;
        try {
            String gmspClassName = (String) conf.get(Config.STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN);
            if (gmspClassName == null) {
                LOG.warn("No group mapper given {}", Config.STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN);
            } else {
                gmsp = ReflectionUtils.newInstance(gmspClassName);
                if (gmsp != null) {
                    gmsp.prepare(conf);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return gmsp;
    }

    /**
     * Get all of the configured Credential Renewer Plugins.
     *
     * @param conf the storm configuration to use.
     * @return the configured credential renewers.
     */
    public static Collection<ICredentialsRenewer> getCredentialRenewers(Map<String, Object> conf) {
        try {
            Set<ICredentialsRenewer> ret = new HashSet<>();
            Collection<String> clazzes = (Collection<String>) conf.get(Config.NIMBUS_CREDENTIAL_RENEWERS);
            if (clazzes != null) {
                for (String clazz : clazzes) {
                    ICredentialsRenewer inst = ReflectionUtils.newInstance(clazz);
                    inst.prepare(conf);
                    ret.add(inst);
                }
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all the Nimbus Auto cred plugins.
     *
     * @param conf nimbus configuration to use.
     * @return nimbus auto credential plugins.
     */
    public static Collection<INimbusCredentialPlugin> getNimbusAutoCredPlugins(Map<String, Object> conf) {
        try {
            Set<INimbusCredentialPlugin> ret = new HashSet<>();
            Collection<String> clazzes = (Collection<String>) conf.get(Config.NIMBUS_AUTO_CRED_PLUGINS);
            if (clazzes != null) {
                for (String clazz : clazzes) {
                    INimbusCredentialPlugin inst = ReflectionUtils.newInstance(clazz);
                    inst.prepare(conf);
                    ret.add(inst);
                }
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all of the configured AutoCredential Plugins.
     *
     * @param topoConf the storm configuration to use.
     * @return the configured auto credentials.
     */
    public static Collection<IAutoCredentials> getAutoCredentials(Map<String, Object> topoConf) {
        try {
            Set<IAutoCredentials> autos = new HashSet<>();
            Collection<String> clazzes = (Collection<String>) topoConf.get(Config.TOPOLOGY_AUTO_CREDENTIALS);
            if (clazzes != null) {
                for (String clazz : clazzes) {
                    IAutoCredentials a = ReflectionUtils.newInstance(clazz);
                    a.prepare(topoConf);
                    autos.add(a);
                }
            }
            LOG.info("Got AutoCreds " + autos);
            return autos;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the key used to store a WorkerToken in the credentials map.
     *
     * @param type the type of service to get.
     * @return the key as a String.
     */
    public static String workerTokenCredentialsKey(WorkerTokenServiceType type) {
        return "STORM_WORKER_TOKEN_" + type.name();
    }

    /**
     * Read a WorkerToken out of credentials for the given type.
     *
     * @param credentials the credentials map.
     * @param type        the type of service we are looking for.
     * @return the deserialized WorkerToken or null if none could be found.
     */
    public static WorkerToken readWorkerToken(Map<String, String> credentials, WorkerTokenServiceType type) {
        WorkerToken ret = null;
        String key = workerTokenCredentialsKey(type);
        String tokenStr = credentials.get(key);
        if (tokenStr != null) {
            ret = Utils.deserializeFromString(tokenStr, WorkerToken.class);
        }
        return ret;
    }

    /**
     * Store a worker token in some credentials. It can be pulled back out by calling readWorkerToken.
     *
     * @param credentials the credentials map.
     * @param token       the token you want to store.
     */
    public static void setWorkerToken(Map<String, String> credentials, WorkerToken token) {
        String key = workerTokenCredentialsKey(token.get_serviceType());
        credentials.put(key, Utils.serializeToString(token));
    }

    /**
     * Find a worker token in a given subject with a given token type.
     *
     * @param subject what to look in.
     * @param type    the type of token to look for.
     * @return the token or null.
     */
    public static WorkerToken findWorkerToken(Subject subject, final WorkerTokenServiceType type) {
        Set<WorkerToken> creds = subject.getPrivateCredentials(WorkerToken.class);
        synchronized (creds) {
            return creds.stream()
                        .filter((wt) ->
                                    wt.get_serviceType() == type)
                        .findAny().orElse(null);
        }
    }

    private static boolean willWorkerTokensBeStoredSecurely(Map<String, Object> conf) {
        boolean overrideZkAuth = ObjectReader.getBoolean(conf.get("TESTING.ONLY.ENABLE.INSECURE.WORKER.TOKENS"), false);
        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
            return true;
        } else if (overrideZkAuth) {
            LOG.error("\n\n\t\tYOU HAVE ENABLED INSECURE WORKER TOKENS.  IF THIS IS NOT A UNIT TEST PLEASE STOP NOW!!!\n\n");
            return true;
        }
        return false;
    }

    /**
     * Check if worker tokens should be enabled on the server side or not.
     *
     * @param server a Thrift server to know if the transport support tokens or not.  No need to create a token if the transport does not
     *               support it.
     * @param conf   the daemon configuration to be sure the tokens are secure.
     * @return true if we can enable them, else false.
     */
    public static boolean areWorkerTokensEnabledServer(ThriftServer server, Map<String, Object> conf) {
        return server.supportsWorkerTokens() && willWorkerTokensBeStoredSecurely(conf);
    }

    /**
     * Check if worker tokens should be enabled on the server side or not (for a given server).
     *
     * @param connectionType the type of server this is for.
     * @param conf           the daemon configuration to be sure the tokens are secure.
     * @return true if we can enable them, else false.
     */
    public static boolean areWorkerTokensEnabledServer(ThriftConnectionType connectionType, Map<String, Object> conf) {
        return connectionType.getWtType() != null && willWorkerTokensBeStoredSecurely(conf);
    }

    /**
     * Turn a WorkerTokenInfo in a byte array.
     *
     * @param wti what to serialize.
     * @return the resulting byte array.
     */
    public static byte[] serializeWorkerTokenInfo(WorkerTokenInfo wti) {
        return Utils.serialize(wti);
    }

    /**
     * Get and deserialize the WorkerTokenInfo in the worker token.
     *
     * @param wt the token.
     * @return the deserialized info.
     */
    public static WorkerTokenInfo getWorkerTokenInfo(WorkerToken wt) {
        return Utils.deserialize(wt.get_info(), WorkerTokenInfo.class);
    }

    //Support for worker tokens Similar to an IAutoCredentials implementation
    private static Subject insertWorkerTokens(Subject subject, Map<String, String> credentials) {
        if (credentials == null) {
            return subject;
        }
        for (WorkerTokenServiceType type : WorkerTokenServiceType.values()) {
            WorkerToken token = readWorkerToken(credentials, type);
            if (token != null) {
                Set<Object> creds = subject.getPrivateCredentials();
                synchronized (creds) {
                    WorkerToken previous = findWorkerToken(subject, type);
                    boolean notAlreadyContained = creds.add(token);
                    if (notAlreadyContained) {
                        if (previous != null) {
                            //this means token is not equal to previous so we should remove previous
                            creds.remove(previous);
                            LOG.info("Replaced WorkerToken for service type {}", type);
                        } else {
                            LOG.info("Added new WorkerToken for service type {}", type);
                        }
                    } else {
                        LOG.info("The new WorkerToken for service type {} is the same as the previous token", type);
                    }
                }
            }
        }
        return subject;
    }

    /**
     * Populate a subject from credentials using the IAutoCredentials.
     *
     * @param subject     the subject to populate or null if a new Subject should be created.
     * @param autos       the IAutoCredentials to call to populate the subject.
     * @param credentials the credentials to pull from
     * @return the populated subject.
     */
    public static Subject populateSubject(Subject subject, Collection<IAutoCredentials> autos, Map<String, String> credentials) {
        try {
            if (subject == null) {
                subject = new Subject();
            }
            for (IAutoCredentials autoCred : autos) {
                autoCred.populateSubject(subject, credentials);
            }
            return insertWorkerTokens(subject, credentials);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Update a subject from credentials using the IAutoCredentials.
     *
     * @param subject     the subject to update
     * @param autos       the IAutoCredentials to call to update the subject.
     * @param credentials the credentials to pull from
     */
    public static void updateSubject(Subject subject, Collection<IAutoCredentials> autos, Map<String, String> credentials) {
        if (subject == null || autos == null) {
            throw new RuntimeException("The subject or auto credentials cannot be null when updating a subject with credentials");
        }

        try {
            for (IAutoCredentials autoCred : autos) {
                autoCred.updateSubject(subject, credentials);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        insertWorkerTokens(subject, credentials);
    }

    /**
     * Construct a transport plugin per storm configuration.
     */
    public static ITransportPlugin getTransportPlugin(ThriftConnectionType type, Map<String, Object> topoConf) {
        try {
            String transportPluginClassName = type.getTransportPlugin(topoConf);
            ITransportPlugin transportPlugin = ReflectionUtils.newInstance(transportPluginClassName);
            transportPlugin.prepare(type, topoConf);
            return transportPlugin;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String makeDigestPayload(Map<String, Object> topoConf, String configSection) {
        String username = null;
        String password = null;
        try {
            Map<String, ?> results = ClientAuthUtils.pullConfig(topoConf, configSection);
            username = (String) results.get(USERNAME);
            password = (String) results.get(PASSWORD);
        } catch (Exception e) {
            LOG.error("Failed to pull username/password out of jaas conf", e);
        }

        if (username == null || password == null) {
            return null;
        }

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-512");
            byte[] output = digest.digest((username + ":" + password).getBytes());
            return Hex.encodeHexString(output);
        } catch (java.security.NoSuchAlgorithmException e) {
            LOG.error("Cant run SHA-512 digest. Algorithm not available.", e);
            throw new RuntimeException(e);
        }
    }



    public static byte[] serializeKerberosTicket(KerberosTicket tgt) throws Exception {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bao);
        out.writeObject(tgt);
        out.flush();
        out.close();
        return bao.toByteArray();
    }

    public static KerberosTicket deserializeKerberosTicket(byte[] tgtBytes) {
        KerberosTicket ret;
        try {

            ByteArrayInputStream bin = new ByteArrayInputStream(tgtBytes);
            ObjectInputStream in = new ObjectInputStream(bin);
            ret = (KerberosTicket) in.readObject();
            in.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    public static KerberosTicket cloneKerberosTicket(KerberosTicket kerberosTicket) {
        if (kerberosTicket != null) {
            try {
                return (deserializeKerberosTicket(serializeKerberosTicket(kerberosTicket)));
            } catch (Exception e) {
                throw new RuntimeException("Failed to clone KerberosTicket TGT!!", e);
            }
        }
        return null;
    }
}
