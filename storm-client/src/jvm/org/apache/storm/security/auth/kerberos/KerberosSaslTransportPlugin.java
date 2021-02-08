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

package org.apache.storm.security.auth.kerberos;

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import org.apache.storm.generated.WorkerToken;
import org.apache.storm.messaging.netty.Login;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.security.auth.sasl.SaslTransportPlugin;
import org.apache.storm.security.auth.sasl.SimpleSaslServerCallbackHandler;
import org.apache.storm.security.auth.workertoken.WorkerTokenAuthorizer;
import org.apache.storm.security.auth.workertoken.WorkerTokenClientCallbackHandler;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.apache.zookeeper.server.auth.KerberosName;
import org.apache.storm.thrift.transport.TSaslClientTransport;
import org.apache.storm.thrift.transport.TSaslServerTransport;
import org.apache.storm.thrift.transport.TTransport;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosSaslTransportPlugin extends SaslTransportPlugin {
    public static final String KERBEROS = "GSSAPI";
    private static final String DIGEST = "DIGEST-MD5";
    private static final Logger LOG = LoggerFactory.getLogger(KerberosSaslTransportPlugin.class);
    private static final String DISABLE_LOGIN_CACHE = "disableLoginCache";
    private static Map<LoginCacheKey, Login> loginCache = new ConcurrentHashMap<>();
    private WorkerTokenAuthorizer workerTokenAuthorizer;

    @Override
    public TTransportFactory getServerTransportFactory(boolean impersonationAllowed) throws IOException {
        if (workerTokenAuthorizer == null) {
            workerTokenAuthorizer = new WorkerTokenAuthorizer(conf, type);
        }
        //create an authentication callback handler
        CallbackHandler serverCallbackHandler = new ServerCallbackHandler(conf, impersonationAllowed);

        String jaasConfFile = ClientAuthUtils.getJaasConf(conf);

        //login our principal
        Subject subject = null;
        try {
            //now login
            Login login = new Login(ClientAuthUtils.LOGIN_CONTEXT_SERVER, serverCallbackHandler, jaasConfFile);
            subject = login.getSubject();
            login.startThreadIfNeeded();
        } catch (LoginException ex) {
            LOG.error("Server failed to login in principal:" + ex, ex);
            throw new RuntimeException(ex);
        }

        //check the credential of our principal
        if (subject.getPrivateCredentials(KerberosTicket.class).isEmpty()) {
            throw new RuntimeException("Fail to verify user principal with section \""
                                       + ClientAuthUtils.LOGIN_CONTEXT_SERVER + "\" in login configuration file " + jaasConfFile);
        }

        String principal = ClientAuthUtils.get(conf, ClientAuthUtils.LOGIN_CONTEXT_SERVER, "principal");
        LOG.debug("principal:" + principal);
        KerberosName serviceKerberosName = new KerberosName(principal);
        String serviceName = serviceKerberosName.getServiceName();
        String hostName = serviceKerberosName.getHostName();
        Map<String, String> props = new TreeMap<>();
        props.put(Sasl.QOP, "auth");
        props.put(Sasl.SERVER_AUTH, "false");

        //create a transport factory that will invoke our auth callback for digest
        TSaslServerTransport.Factory factory = new TSaslServerTransport.Factory();
        factory.addServerDefinition(KERBEROS, serviceName, hostName, props, serverCallbackHandler);

        //Also add in support for worker tokens
        factory.addServerDefinition(DIGEST, ClientAuthUtils.SERVICE, hostName, null,
                                    new SimpleSaslServerCallbackHandler(impersonationAllowed, workerTokenAuthorizer));

        //create a wrap transport factory so that we could apply user credential during connections
        TUGIAssumingTransportFactory wrapFactory = new TUGIAssumingTransportFactory(factory, subject);

        LOG.info("SASL GSSAPI transport factory will be used");
        return wrapFactory;
    }

    private Login mkLogin() throws IOException {
        try {
            //create an authentication callback handler
            ClientCallbackHandler clientCallbackHandler = new ClientCallbackHandler(conf);
            //now login
            Login login = new Login(ClientAuthUtils.LOGIN_CONTEXT_CLIENT, clientCallbackHandler, ClientAuthUtils.getJaasConf(conf));
            login.startThreadIfNeeded();
            return login;
        } catch (LoginException ex) {
            LOG.error("Server failed to login in principal:" + ex, ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public TTransport connect(TTransport transport, String serverHost, String asUser) throws IOException, TTransportException {
        WorkerToken token = WorkerTokenClientCallbackHandler.findWorkerTokenInSubject(type);
        if (token != null) {
            CallbackHandler clientCallbackHandler = new WorkerTokenClientCallbackHandler(token);
            TSaslClientTransport wrapperTransport = new TSaslClientTransport(DIGEST,
                                                                             null,
                                                                             ClientAuthUtils.SERVICE,
                                                                             serverHost,
                                                                             null,
                                                                             clientCallbackHandler,
                                                                             transport);
            wrapperTransport.open();
            LOG.debug("SASL DIGEST-MD5 WorkerToken client transport has been established");

            return wrapperTransport;
        }
        return kerberosConnect(transport, serverHost, asUser);
    }

    private TTransport kerberosConnect(TTransport transport, String serverHost, String asUser) throws IOException {
        //login our user
        SortedMap<String, ?> authConf = ClientAuthUtils.pullConfig(conf, ClientAuthUtils.LOGIN_CONTEXT_CLIENT);
        if (authConf == null) {
            throw new RuntimeException("Error in parsing the kerberos login Configuration, returned null");
        }

        boolean disableLoginCache = false;
        if (authConf.containsKey(DISABLE_LOGIN_CACHE)) {
            disableLoginCache = Boolean.valueOf((String) authConf.get(DISABLE_LOGIN_CACHE));
        }

        Login login;
        LoginCacheKey key = new LoginCacheKey(authConf);
        if (disableLoginCache) {
            LOG.debug("Kerberos Login Cache is disabled, attempting to contact the Kerberos Server");
            login = mkLogin();
            //this is to prevent the potential bug that
            //if the Login Cache is (1) enabled, and then (2) disabled and then (3) enabled again,
            //and if the LoginCacheKey remains unchanged, (3) will use the Login cache from (1), which could be wrong,
            //because the TGT cache (as well as the principle) could have been changed during (2)
            loginCache.remove(key);
        } else {
            LOG.debug("Trying to get the Kerberos Login from the Login Cache");
            login = loginCache.get(key);
            if (login == null) {
                synchronized (loginCache) {
                    login = loginCache.get(key);
                    if (login == null) {
                        LOG.debug("Kerberos Login was not found in the Login Cache, attempting to contact the Kerberos Server");
                        login = mkLogin();
                        loginCache.put(key, login);
                    }
                }
            }
        }

        final Subject subject = login.getSubject();
        if (subject.getPrivateCredentials(KerberosTicket.class).isEmpty()) { //error
            throw new RuntimeException("Fail to verify user principal with section \""
                    + ClientAuthUtils.LOGIN_CONTEXT_CLIENT + "\" in login configuration file " + ClientAuthUtils.getJaasConf(conf));
        }

        final String principal = StringUtils.isBlank(asUser) ? getPrincipal(subject) : asUser;
        String serviceName = ClientAuthUtils.get(conf, ClientAuthUtils.LOGIN_CONTEXT_CLIENT, "serviceName");
        if (serviceName == null) {
            serviceName = ClientAuthUtils.SERVICE;
        }
        Map<String, String> props = new TreeMap<>();
        props.put(Sasl.QOP, "auth");
        props.put(Sasl.SERVER_AUTH, "false");

        LOG.debug("SASL GSSAPI client transport is being established");
        final TTransport sasalTransport = new TSaslClientTransport(KERBEROS,
                                                                   principal,
                                                                   serviceName,
                                                                   serverHost,
                                                                   props,
                                                                   null,
                                                                   transport);

        //open Sasl transport with the login credential
        try {
            Subject.doAs(subject,
                    new PrivilegedExceptionAction<Void>() {
                        @Override
                        public Void run() {
                            try {
                                LOG.debug("do as:" + principal);
                                sasalTransport.open();
                            } catch (Exception e) {
                                LOG.error("Client failed to open SaslClientTransport to interact with a server during "
                                                + "session initiation: "
                                                + e,
                                        e);
                        }
                        return null;
                    }
                });
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e);
        }

        return sasalTransport;
    }

    private String getPrincipal(Subject subject) {
        Set<Principal> principals = (Set<Principal>) subject.getPrincipals();
        if (principals == null || principals.size() < 1) {
            LOG.info("No principal found in login subject");
            return null;
        }
        return ((Principal) (principals.toArray()[0])).getName();
    }

    @Override
    public boolean areWorkerTokensSupported() {
        return true;
    }

    @Override
    public void close() {
        workerTokenAuthorizer.close();
    }

    /**
     * A TransportFactory that wraps another one, but assumes a specified UGI before calling through.
     *
     * <p>This is used on the server side to assume the server's Principal when accepting clients.
     */
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    static class TUGIAssumingTransportFactory extends TTransportFactory {
        private final Subject subject;
        private final TTransportFactory wrapped;

        TUGIAssumingTransportFactory(TTransportFactory wrapped, Subject subject) {
            this.wrapped = wrapped;
            this.subject = subject;

            Set<Principal> principals = (Set<Principal>) subject.getPrincipals();
            if (principals.size() > 0) {
                LOG.info("Service principal:" + ((Principal) (principals.toArray()[0])).getName());
            }
        }

        @Override
        public TTransport getTransport(final TTransport trans) {
            try {
                return Subject.doAs(subject,
                    (PrivilegedExceptionAction<TTransport>) () -> {
                        try {
                            return wrapped.getTransport(trans);
                        } catch (Exception e) {
                            LOG.debug("Storm server failed to open transport to interact with a client during "
                                            + "session initiation: "
                                            + e,
                                    e);
                            return new NoOpTTrasport(null);
                        }
                    });
            } catch (PrivilegedActionException e) {
                LOG.error("Storm server experienced a PrivilegedActionException exception while creating a transport "
                                + "using a JAAS principal context:"
                                + e,
                        e);
                return null;
            }
        }
    }

    private class LoginCacheKey {
        private String keyString = null;

        LoginCacheKey(SortedMap<String, ?> authConf) throws IOException {
            if (authConf != null) {
                StringBuilder stringBuilder = new StringBuilder();
                for (String configKey : authConf.keySet()) {
                    //DISABLE_LOGIN_CACHE indicates whether or not to use the LoginCache.
                    //So we exclude it from the keyString
                    if (configKey.equals(DISABLE_LOGIN_CACHE)) {
                        continue;
                    }
                    String configValue = (String) authConf.get(configKey);
                    stringBuilder.append(configKey);
                    stringBuilder.append(configValue);
                }
                keyString = stringBuilder.toString();
            } else {
                throw new IllegalArgumentException("Configuration should not be null");
            }
        }

        @Override
        public int hashCode() {
            return keyString.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof LoginCacheKey) && keyString.equals(((LoginCacheKey) obj).keyString);
        }

        @Override
        public String toString() {
            return (keyString);
        }
    }
}
