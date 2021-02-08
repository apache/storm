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

import com.codahale.metrics.Gauge;
import java.lang.reflect.Method;
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

    @Override
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

            LOG.info("Invoking Hadoop UserGroupInformation.loginUserFromSubject.");
            Method login = ugi.getMethod("loginUserFromSubject", Subject.class);
            login.invoke(null, subject);

            //Refer to STORM-3606 for details
            LOG.warn("UserGroupInformation.loginUserFromSubject will spawn a TGT renewal thread (\"TGT Renewer for <username>\") "
                    + "to execute \"kinit -R\" command some time before the current TGT expires. "
                    + "It will fail because TGT is not in the local TGT cache and the thread will eventually abort. "
                    + "Exceptions from this TGT renewal thread can be ignored. Note: TGT for the Worker is kept in memory. "
                    + "Please refer to STORM-3606 for detailed explanations");
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
        topoContext.registerGauge("TGT-TimeToExpiryMsecs", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return getMsecsUntilExpiration();
            }
        });
    }
}
