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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.DestroyFailedException;
import javax.security.auth.RefreshFailedException;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.xml.bind.DatatypeConverter;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsRegistrant;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.security.auth.ICredentialsRenewer;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Automatically take a user's TGT, and push it, and renew it in Nimbus.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class AutoTGT implements IAutoCredentials, ICredentialsRenewer, IMetricsRegistrant {
    protected static final AtomicReference<KerberosTicket> kerbTicket = new AtomicReference<>();
    private static final Logger LOG = LoggerFactory.getLogger(AutoTGT.class);
    private static final float TICKET_RENEW_WINDOW = 0.80f;
    private Map<String, Object> conf;
    private Map<String, String> credentials;

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    private static KerberosTicket getTGT(Subject subject) {
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        for (KerberosTicket ticket : tickets) {
            KerberosPrincipal server = ticket.getServer();
            if (server.getName().equals("krbtgt/" + server.getRealm() + "@" + server.getRealm())) {
                return ticket;
            }
        }
        return null;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static KerberosTicket getTGT(Map<String, String> credentials) {
        KerberosTicket ret = null;
        if (credentials != null && credentials.containsKey("TGT") && credentials.get("TGT") != null) {
            ret = ClientAuthUtils.deserializeKerberosTicket(DatatypeConverter.parseBase64Binary(credentials.get("TGT")));
        }
        return ret;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static void saveTGT(KerberosTicket tgt, Map<String, String> credentials) {
        try {

            byte[] bytes = ClientAuthUtils.serializeKerberosTicket(tgt);
            credentials.put("TGT", DatatypeConverter.printBase64Binary(bytes));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void clearCredentials(Subject subject, KerberosTicket tgt) {
        Set<Object> creds = subject.getPrivateCredentials();
        synchronized (creds) {
            Iterator<Object> iterator = creds.iterator();
            while (iterator.hasNext()) {
                Object o = iterator.next();
                if (o instanceof KerberosTicket) {
                    KerberosTicket t = (KerberosTicket) o;
                    iterator.remove();
                    try {
                        t.destroy();
                    } catch (DestroyFailedException e) {
                        LOG.warn("Failed to destory ticket ", e);
                    }
                }
            }
            if (tgt != null) {
                creds.add(tgt);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        AutoTGT at = new AutoTGT();
        Map<String, Object> conf = new HashMap();
        conf.put("java.security.auth.login.config", args[0]);
        at.prepare(conf);
        Map<String, String> creds = new HashMap<>();
        at.populateCredentials(creds);
        Subject s = new Subject();
        at.populateSubject(s, creds);
        LOG.info("Got a Subject " + s);
    }

    public void prepare(Map<String, Object> conf) {
        this.conf = conf;
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        this.credentials = credentials;
        //Log the user in and get the TGT
        try {
            Configuration loginConf = ClientAuthUtils.getConfiguration(conf);
            ClientCallbackHandler clientCallbackHandler = new ClientCallbackHandler(conf);

            //login our user
            LoginContext lc = new LoginContext(ClientAuthUtils.LOGIN_CONTEXT_CLIENT, null, clientCallbackHandler, loginConf);
            try {
                lc.login();
                final Subject subject = lc.getSubject();
                KerberosTicket tgt = getTGT(subject);

                if (tgt == null) { //error
                    throw new RuntimeException("Fail to verify user principal with section \""
                                               + ClientAuthUtils.LOGIN_CONTEXT_CLIENT + "\" in login configuration file " + loginConf);
                }

                if (!tgt.isForwardable()) {
                    throw new RuntimeException("The TGT found is not forwardable. Please use -f option with 'kinit'.");
                }

                if (!tgt.isRenewable()) {
                    throw new RuntimeException("The TGT found is not renewable. Please use -r option with 'kinit'.");
                }

                if (tgt.getClientAddresses() != null) {
                    throw new RuntimeException("The TGT found is not address-less. Please use -A option with 'kinit'.");
                }

                LOG.info("Pushing TGT for " + tgt.getClient() + " to topology.");
                saveTGT(tgt, credentials);
            } finally {
                lc.logout();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        this.credentials = credentials;
        populateSubjectWithTGT(subject, credentials);
    }

    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        this.credentials = credentials;
        populateSubjectWithTGT(subject, credentials);
        loginHadoopUser(subject);
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    private void populateSubjectWithTGT(Subject subject, Map<String, String> credentials) {
        LOG.info("Populating TGT from credentials");
        KerberosTicket tgt = getTGT(credentials);
        if (tgt != null) {
            clearCredentials(subject, tgt);
            subject.getPrincipals().add(tgt.getClient());
            kerbTicket.set(tgt);
        } else {
            LOG.info("No TGT found in credentials");
        }
    }

    /**
     * Hadoop does not just go off of a TGT, it needs a bit more.  This should fill in the rest.
     *
     * @param subject the subject that should have a TGT in it.
     */
    private void loginHadoopUser(Subject subject) {
        Class<?> ugi;
        try {
            ugi = Class.forName("org.apache.hadoop.security.UserGroupInformation");
        } catch (ClassNotFoundException e) {
            LOG.info("Hadoop was not found on the class path");
            return;
        }
        try {
            Method isSecEnabled = ugi.getMethod("isSecurityEnabled");
            if (!((Boolean) isSecEnabled.invoke(null))) {
                LOG.warn("Hadoop is on the classpath but not configured for "
                         + "security, if you want security you need to be sure that "
                         + "hadoop.security.authentication=kerberos in core-site.xml "
                         + "in your jar");
                return;
            }

            // We are just trying to do the following:
            //
            // Configuration conf = new Configuration();
            // HadoopKerberosName.setConfiguration(conf);
            // subject.getPrincipals().add(new User(tgt.getClient().toString(), AuthenticationMethod.KERBEROS, null));

            Class<?> confClass = Class.forName("org.apache.hadoop.conf.Configuration");
            Constructor confCons = confClass.getConstructor();
            Object conf = confCons.newInstance();
            Class<?> hknClass = Class.forName("org.apache.hadoop.security.HadoopKerberosName");
            Method hknSetConf = hknClass.getMethod("setConfiguration", confClass);
            hknSetConf.invoke(null, conf);

            Class<?> authMethodClass = Class.forName("org.apache.hadoop.security.UserGroupInformation$AuthenticationMethod");
            Object kerbAuthMethod = null;
            for (Object authMethod : authMethodClass.getEnumConstants()) {
                if ("KERBEROS".equals(authMethod.toString())) {
                    kerbAuthMethod = authMethod;
                    break;
                }
            }

            Class<?> userClass = Class.forName("org.apache.hadoop.security.User");
            Constructor userCons = userClass.getConstructor(String.class, authMethodClass, LoginContext.class);
            userCons.setAccessible(true);
            String name = getTGT(subject).getClient().toString();
            Object user = userCons.newInstance(name, kerbAuthMethod, null);
            subject.getPrincipals().add((Principal) user);

        } catch (Exception e) {
            LOG.error("Something went wrong while trying to initialize Hadoop through reflection. This version of hadoop "
                     + "may not be compatible.", e);
        }
    }

    private long getRefreshTime(KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long end = tgt.getEndTime().getTime();
        return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
    }

    @Override
    public void renew(Map<String, String> credentials, Map<String, Object> topologyConf, String topologyOwnerPrincipal) {
        this.credentials = credentials;
        KerberosTicket tgt = getTGT(credentials);
        if (tgt != null) {
            long refreshTime = getRefreshTime(tgt);
            long now = System.currentTimeMillis();
            if (now >= refreshTime) {
                try {
                    LOG.info("Renewing TGT for " + tgt.getClient());
                    tgt.refresh();
                    saveTGT(tgt, credentials);
                } catch (RefreshFailedException e) {
                    LOG.warn("Failed to refresh TGT", e);
                }
            }
        }
    }

    private Long getMsecsUntilExpiration() {
        KerberosTicket tgt = getTGT(this.credentials);
        if (tgt == null) {
            return null;
        }
        long end = tgt.getEndTime().getTime();
        return end - System.currentTimeMillis();
    }

    @Override
    public void registerMetrics(TopologyContext topoContext, Map<String, Object> topoConf) {
        int bucketSize = ((Number) topoConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)).intValue();
        topoContext.registerMetric("TGT-TimeToExpiryMsecs", () -> getMsecsUntilExpiration(), bucketSize);
    }
}
