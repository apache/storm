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

package org.apache.storm.security.auth;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.apache.storm.Testing;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.WorkerToken;
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.security.auth.authorizer.ImpersonationAuthorizer;
import org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer;
import org.apache.storm.security.auth.authorizer.SimpleWhitelistAuthorizer;
import org.apache.storm.security.auth.digest.DigestSaslTransportPlugin;
import org.apache.storm.security.auth.workertoken.WorkerTokenManager;
import org.apache.storm.testing.InProcessZookeeper;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AuthTest {
    //3 seconds in milliseconds
    public static final int NIMBUS_TIMEOUT = 3_000;
    private static final Logger LOG = LoggerFactory.getLogger(AuthTest.class);
    private static final File BASE = new File("./src/test/resources/");
    private static final String DIGEST_JAAS_CONF = new File(BASE, "jaas_digest.conf").getAbsolutePath();
    private static final String BAD_PASSWORD_CONF = new File(BASE, "jaas_digest_bad_password.conf").getAbsolutePath();
    private static final String WRONG_USER_CONF = new File(BASE, "jaas_digest_unknown_user.conf").getAbsolutePath();
    private static final String MISSING_CLIENT = new File(BASE, "jaas_digest_missing_client.conf").getAbsolutePath();

    public static Principal mkPrincipal(final String name) {
        return new Principal() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public boolean equals(Object other) {
                return other instanceof Principal
                       && name.equals(((Principal) other).getName());
            }

            @Override
            public String toString() {
                return name;
            }

            @Override
            public int hashCode() {
                return name.hashCode();
            }
        };
    }

    public static Subject mkSubject(String name) {
        return new Subject(true, Collections.singleton(mkPrincipal(name)),
                           Collections.emptySet(), Collections.emptySet());
    }

    public static void withServer(Class<? extends ITransportPlugin> transportPluginClass,
                                  Nimbus.Iface impl,
                                  MyBiConsumer<ThriftServer, Map<String, Object>> body) throws Exception {
        withServer(null, transportPluginClass, impl, null, null, body);
    }

    public static void withServer(String loginCfg,
                                  Class<? extends ITransportPlugin> transportPluginClass,
                                  Nimbus.Iface impl,
                                  MyBiConsumer<ThriftServer, Map<String, Object>> body) throws Exception {
        withServer(loginCfg, transportPluginClass, impl, null, null, body);
    }

    public static void withServer(String loginCfg,
                                  Class<? extends ITransportPlugin> transportPluginClass,
                                  Nimbus.Iface impl,
                                  InProcessZookeeper zk,
                                  Map<String, Object> extraConfs,
                                  MyBiConsumer<ThriftServer, Map<String, Object>> body) throws Exception {
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        conf.put(Config.NIMBUS_THRIFT_PORT, 0);
        conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, transportPluginClass.getName());

        if (loginCfg != null) {
            conf.put("java.security.auth.login.config", loginCfg);
        }

        if (zk != null) {
            conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
            conf.put(Config.STORM_ZOOKEEPER_PORT, zk.getPort());
        }

        if (extraConfs != null) {
            conf.putAll(extraConfs);
        }

        Nimbus.Iface handler = impl != null ? impl : mock(Nimbus.Iface.class);
        final ThriftServer server = new ThriftServer(conf,
                                                     new Nimbus.Processor<>(handler),
                                                     ThriftConnectionType.NIMBUS);

        LOG.info("Created Server... {}", server);
        new Thread(() -> {
            LOG.info("Starting Serving...");
            server.serve();
        }).start();
        Testing.whileTimeout(
            () -> !server.isServing(),
            () -> {
                try {
                    Time.sleep(100);
                } catch (InterruptedException e) {
                    //Ignored
                }
            });
        try {
            LOG.info("Starting to run {}", body);
            body.accept(server, conf);
            LOG.info("{} finished with no exceptions", body);
        } finally {
            LOG.info("Stopping server {}", server);
            server.stop();
        }
    }

    public static void verifyIncorrectJaasConf(ThriftServer server, Map<String, Object> conf, String jaas,
                                               Class<? extends Exception> expectedException) {
        Map<String, Object> badConf = new HashMap<>(conf);
        badConf.put("java.security.auth.login.config", jaas);
        try (NimbusClient client = new NimbusClient(badConf, "localhost", server.getPort(), NIMBUS_TIMEOUT)) {
            client.getClient().activate("bad_auth_test_topology");
            fail("An exception should have been thrown trying to connect.");
        } catch (Exception e) {
            LOG.info("Got Exception...", e);
            assert (Utils.exceptionCauseIsInstanceOf(expectedException, e));
        }
    }

    public static Subject createSubjectWith(WorkerToken wt) {
        //This is a bit ugly, but it shows how this would happen in a worker so we will use the same APIs
        Map<String, String> creds = new HashMap<>();
        ClientAuthUtils.setWorkerToken(creds, wt);
        Subject subject = new Subject();
        ClientAuthUtils.updateSubject(subject, Collections.emptyList(), creds);
        return subject;
    }

    public static void tryConnectAs(Map<String, Object> conf, ThriftServer server, Subject subject, String topoId)
        throws PrivilegedActionException {
        Subject.doAs(subject, (PrivilegedExceptionAction<Void>) () -> {
            try (NimbusClient client = new NimbusClient(conf, "localhost", server.getPort(), NIMBUS_TIMEOUT)) {
                client.getClient().activate(topoId); //Yes this should be a topo name, but it makes this simpler...
            }
            return null;
        });
    }

    public static Subject testConnectWithTokenFor(WorkerTokenManager wtMan, Map<String, Object> conf, ThriftServer server,
                                                  String user, String topoId) throws PrivilegedActionException {
        WorkerToken wt = wtMan.createOrUpdateTokenFor(WorkerTokenServiceType.NIMBUS, user, topoId);
        Subject subject = createSubjectWith(wt);
        tryConnectAs(conf, server, subject, topoId);
        return subject;
    }

    public static void verifyUserIs(AtomicReference<ReqContext> user, String userName) {
        //The user from the token is bob, so verify that the name was set correctly...
        ReqContext found = user.get();
        assertNotNull(found);
        assertEquals(userName, found.principal().getName());
        assertFalse(found.isImpersonating());
        user.set(null);
    }

    public static ReqContext mkImpersonatingReqContext(String impersonatingUser, String userBeingIUmpersonated, InetAddress remoteAddress) {
        ReqContext ret = new ReqContext(mkSubject(userBeingIUmpersonated));
        ret.setRemoteAddress(remoteAddress);
        ret.setRealPrincipal(mkPrincipal(impersonatingUser));
        return ret;
    }

    @Test
    public void kerbToLocalTest() {
        KerberosPrincipalToLocal kptol = new KerberosPrincipalToLocal();
        kptol.prepare(Collections.emptyMap());
        assertEquals("me", kptol.toLocal(mkPrincipal("me@realm")));
        assertEquals("simple", kptol.toLocal(mkPrincipal("simple")));
        assertEquals("someone", kptol.toLocal(mkPrincipal("someone/host@realm")));
    }

    @Test
    public void simpleAuthTest() throws Exception {
        Nimbus.Iface impl = mock(Nimbus.Iface.class);
        withServer(SimpleTransportPlugin.class,
                   impl,
                   (ThriftServer server, Map<String, Object> conf) -> {
                       try (NimbusClient client = new NimbusClient(conf, "localhost", server.getPort(), NIMBUS_TIMEOUT)) {
                           client.getClient().activate("security_auth_test_topology");
                       }

                       //Verify digest is rejected...
                       Map<String, Object> badConf = new HashMap<>(conf);
                       badConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, DigestSaslTransportPlugin.class.getName());
                       badConf.put("java.security.auth.login.config", DIGEST_JAAS_CONF);
                       badConf.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);
                       try (NimbusClient client = new NimbusClient(badConf, "localhost", server.getPort(), NIMBUS_TIMEOUT)) {
                           client.getClient().activate("bad_security_auth_test_topology");
                           fail("An exception should have been thrown trying to connect.");
                       } catch (Exception te) {
                           LOG.info("Got Exception...", te);
                           assert (Utils.exceptionCauseIsInstanceOf(TTransportException.class, te));
                       }
                   });
        verify(impl).activate("security_auth_test_topology");
        verify(impl, never()).activate("bad_security_auth_test_topology");
    }

    @Test
    public void digestAuthTest() throws Exception {
        Nimbus.Iface impl = mock(Nimbus.Iface.class);
        final AtomicReference<ReqContext> user = new AtomicReference<>();
        doAnswer((invocation) -> {
            user.set(new ReqContext(ReqContext.context()));
            return null;
        }).when(impl).activate(anyString());

        withServer(DIGEST_JAAS_CONF,
                   DigestSaslTransportPlugin.class,
                   impl,
                   (ThriftServer server, Map<String, Object> conf) -> {
                       try (NimbusClient client = new NimbusClient(conf, "localhost", server.getPort(), NIMBUS_TIMEOUT)) {
                           client.getClient().activate("security_auth_test_topology");
                       }

                       conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);

                       //Verify simple is rejected...
                       Map<String, Object> badTransport = new HashMap<>(conf);
                       badTransport.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, SimpleTransportPlugin.class.getName());
                       try (NimbusClient client = new NimbusClient(badTransport, "localhost", server.getPort(), NIMBUS_TIMEOUT)) {
                           client.getClient().activate("bad_security_auth_test_topology");
                           fail("An exception should have been thrown trying to connect.");
                       } catch (Exception te) {
                           LOG.info("Got Exception...", te);
                           assert (Utils.exceptionCauseIsInstanceOf(TTransportException.class, te));
                       }
                       //The user here from the jaas conf is bob.  No impersonation is done, so verify that
                       ReqContext found = user.get();
                       assertNotNull(found);
                       assertEquals("bob", found.principal().getName());
                       assertFalse(found.isImpersonating());
                       user.set(null);

                       verifyIncorrectJaasConf(server, conf, BAD_PASSWORD_CONF, TTransportException.class);
                       verifyIncorrectJaasConf(server, conf, WRONG_USER_CONF, TTransportException.class);
                       verifyIncorrectJaasConf(server, conf, "./nonexistent.conf", RuntimeException.class);
                       verifyIncorrectJaasConf(server, conf, MISSING_CLIENT, IOException.class);
                   });
        verify(impl).activate("security_auth_test_topology");
        verify(impl, never()).activate("bad_auth_test_topology");
    }

    @Test
    public void workerTokenDigestAuthTest() throws Exception {
        LOG.info("\n\n\t\tworkerTokenDigestAuthTest - START\n\n");
        Nimbus.Iface impl = mock(Nimbus.Iface.class);
        final AtomicReference<ReqContext> user = new AtomicReference<>();
        doAnswer((invocation) -> {
            user.set(new ReqContext(ReqContext.context()));
            return null;
        }).when(impl).activate(anyString());

        Map<String, Object> extraConfs = new HashMap<>();
        //Let worker tokens work on insecure ZK...
        extraConfs.put("TESTING.ONLY.ENABLE.INSECURE.WORKER.TOKENS", true);

        try (InProcessZookeeper zk = new InProcessZookeeper()) {
            withServer(MISSING_CLIENT,
                       DigestSaslTransportPlugin.class,
                       impl,
                       zk,
                       extraConfs,
                       (ThriftServer server, Map<String, Object> conf) -> {
                           try (Time.SimulatedTime sim = new Time.SimulatedTime()) {
                               conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);
                               //We cannot connect if there is no client section in the jaas conf...
                               try (NimbusClient client = new NimbusClient(conf, "localhost", server.getPort(), NIMBUS_TIMEOUT)) {
                                   client.getClient().activate("bad_auth_test_topology");
                                   fail("We should not be able to connect without a token...");
                               } catch (Exception e) {
                                   assert (Utils.exceptionCauseIsInstanceOf(IOException.class, e));
                               }

                               //Now lets create a token and verify that we can connect...
                               IStormClusterState state =
                                   ClusterUtils.mkStormClusterState(conf, new ClusterStateContext(DaemonType.NIMBUS, conf));
                               WorkerTokenManager wtMan = new WorkerTokenManager(conf, state);
                               Subject bob = testConnectWithTokenFor(wtMan, conf, server, "bob", "topo-bob");
                               verifyUserIs(user, "bob");

                               Time.advanceTimeSecs(TimeUnit.HOURS.toSeconds(12));

                               //Alice has no digest jaas section at all...
                               Subject alice = testConnectWithTokenFor(wtMan, conf, server, "alice", "topo-alice");
                               verifyUserIs(user, "alice");

                               Time.advanceTimeSecs(TimeUnit.HOURS.toSeconds(13));
                               //Verify that bob's token has expired

                               try {
                                   tryConnectAs(conf, server, bob, "bad_auth_test_topology");
                                   fail("We should not be able to connect with bad auth");
                               } catch (Exception e) {
                                   assert (Utils.exceptionCauseIsInstanceOf(TTransportException.class, e));
                               }
                               tryConnectAs(conf, server, alice, "topo-alice");
                               verifyUserIs(user, "alice");

                               //Now see if we can create a new token for bob and try again.
                               bob = testConnectWithTokenFor(wtMan, conf, server, "bob", "topo-bob");
                               verifyUserIs(user, "bob");

                               tryConnectAs(conf, server, alice, "topo-alice");
                               verifyUserIs(user, "alice");
                           }
                       });
        }
        verify(impl, times(2)).activate("topo-bob");
        verify(impl, times(3)).activate("topo-alice");
        verify(impl, never()).activate("bad_auth_test_topology");
        LOG.info("\n\n\t\tworkerTokenDigestAuthTest - END\n\n");
    }

    @Test
    public void negativeWhitelistAuthroizationTest() {
        SimpleWhitelistAuthorizer auth = new SimpleWhitelistAuthorizer();
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        auth.prepare(conf);
        ReqContext context = new ReqContext(mkSubject("user"));
        assertFalse(auth.permit(context, "activate", conf));
    }

    @Test
    public void positiveWhitelistAuthroizationTest() {
        SimpleWhitelistAuthorizer auth = new SimpleWhitelistAuthorizer();
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        conf.put(SimpleWhitelistAuthorizer.WHITELIST_USERS_CONF, Arrays.asList("user"));
        auth.prepare(conf);
        ReqContext context = new ReqContext(mkSubject("user"));
        assertTrue(auth.permit(context, "activate", conf));
    }

    @Test
    public void simpleAclUserAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        clusterConf.put(Config.NIMBUS_ADMINS, Arrays.asList("admin"));
        clusterConf.put(Config.NIMBUS_SUPERVISOR_USERS, Arrays.asList("supervisor"));
        ReqContext admin = new ReqContext(mkSubject("admin"));
        ReqContext supervisor = new ReqContext(mkSubject("supervisor"));
        ReqContext userA = new ReqContext(mkSubject("user-a"));
        ReqContext userB = new ReqContext(mkSubject("user-b"));

        final Map<String, Object> empty = Collections.emptyMap();
        final Map<String, Object> aAllowed = new HashMap<>();
        aAllowed.put(Config.TOPOLOGY_USERS, Arrays.asList("user-a"));

        SimpleACLAuthorizer authorizer = new SimpleACLAuthorizer();
        authorizer.prepare(clusterConf);

        assertTrue(authorizer.permit(userA, "submitTopology", empty));
        assertTrue(authorizer.permit(userB, "submitTopology", empty));
        assertTrue(authorizer.permit(admin, "submitTopology", empty));
        assertFalse(authorizer.permit(supervisor, "submitTopology", empty));

        assertTrue(authorizer.permit(userA, "fileUpload", null));
        assertTrue(authorizer.permit(userB, "fileUpload", null));
        assertTrue(authorizer.permit(admin, "fileUpload", null));
        assertFalse(authorizer.permit(supervisor, "fileUpload", null));

        assertTrue(authorizer.permit(userA, "getNimbusConf", null));
        assertTrue(authorizer.permit(userB, "getNimbusConf", null));
        assertTrue(authorizer.permit(admin, "getNimbusConf", null));
        assertFalse(authorizer.permit(supervisor, "getNimbusConf", null));

        assertTrue(authorizer.permit(userA, "getClusterInfo", null));
        assertTrue(authorizer.permit(userB, "getClusterInfo", null));
        assertTrue(authorizer.permit(admin, "getClusterInfo", null));
        assertFalse(authorizer.permit(supervisor, "getClusterInfo", null));

        assertFalse(authorizer.permit(userA, "fileDownload", null));
        assertFalse(authorizer.permit(userB, "fileDownload", null));
        assertTrue(authorizer.permit(admin, "fileDownload", null));
        assertTrue(authorizer.permit(supervisor, "fileDownload", null));

        assertTrue(authorizer.permit(userA, "killTopology", aAllowed));
        assertFalse(authorizer.permit(userB, "killTopology", aAllowed));
        assertTrue(authorizer.permit(admin, "killTopology", aAllowed));
        assertFalse(authorizer.permit(supervisor, "killTopology", aAllowed));

        assertTrue(authorizer.permit(userA, "uploadNewCredentials", aAllowed));
        assertFalse(authorizer.permit(userB, "uploadNewCredentials", aAllowed));
        assertTrue(authorizer.permit(admin, "uploadNewCredentials", aAllowed));
        assertFalse(authorizer.permit(supervisor, "uploadNewCredentials", aAllowed));

        assertTrue(authorizer.permit(userA, "rebalance", aAllowed));
        assertFalse(authorizer.permit(userB, "rebalance", aAllowed));
        assertTrue(authorizer.permit(admin, "rebalance", aAllowed));
        assertFalse(authorizer.permit(supervisor, "rebalance", aAllowed));

        assertTrue(authorizer.permit(userA, "activate", aAllowed));
        assertFalse(authorizer.permit(userB, "activate", aAllowed));
        assertTrue(authorizer.permit(admin, "activate", aAllowed));
        assertFalse(authorizer.permit(supervisor, "activate", aAllowed));

        assertTrue(authorizer.permit(userA, "deactivate", aAllowed));
        assertFalse(authorizer.permit(userB, "deactivate", aAllowed));
        assertTrue(authorizer.permit(admin, "deactivate", aAllowed));
        assertFalse(authorizer.permit(supervisor, "deactivate", aAllowed));

        assertTrue(authorizer.permit(userA, "getTopologyConf", aAllowed));
        assertFalse(authorizer.permit(userB, "getTopologyConf", aAllowed));
        assertTrue(authorizer.permit(admin, "getTopologyConf", aAllowed));
        assertFalse(authorizer.permit(supervisor, "getTopologyConf", aAllowed));

        assertTrue(authorizer.permit(userA, "getTopology", aAllowed));
        assertFalse(authorizer.permit(userB, "getTopology", aAllowed));
        assertTrue(authorizer.permit(admin, "getTopology", aAllowed));
        assertFalse(authorizer.permit(supervisor, "getTopology", aAllowed));

        assertTrue(authorizer.permit(userA, "getUserTopology", aAllowed));
        assertFalse(authorizer.permit(userB, "getUserTopology", aAllowed));
        assertTrue(authorizer.permit(admin, "getUserTopology", aAllowed));
        assertFalse(authorizer.permit(supervisor, "getUserTopology", aAllowed));

        assertTrue(authorizer.permit(userA, "getTopologyInfo", aAllowed));
        assertFalse(authorizer.permit(userB, "getTopologyInfo", aAllowed));
        assertTrue(authorizer.permit(admin, "getTopologyInfo", aAllowed));
        assertFalse(authorizer.permit(supervisor, "getTopologyInfo", aAllowed));
    }

    @Test
    public void simpleAclNimbusUsersAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        clusterConf.put(Config.NIMBUS_ADMINS, Arrays.asList("admin"));
        clusterConf.put(Config.NIMBUS_SUPERVISOR_USERS, Arrays.asList("supervisor"));
        clusterConf.put(Config.NIMBUS_USERS, Arrays.asList("user-a"));
        ReqContext admin = new ReqContext(mkSubject("admin"));
        ReqContext supervisor = new ReqContext(mkSubject("supervisor"));
        ReqContext userA = new ReqContext(mkSubject("user-a"));
        ReqContext userB = new ReqContext(mkSubject("user-b"));

        final Map<String, Object> empty = Collections.emptyMap();

        SimpleACLAuthorizer authorizer = new SimpleACLAuthorizer();
        authorizer.prepare(clusterConf);

        assertTrue(authorizer.permit(userA, "submitTopology", empty));
        assertFalse(authorizer.permit(userB, "submitTopology", empty));
        assertTrue(authorizer.permit(admin, "fileUpload", null));
        assertTrue(authorizer.permit(supervisor, "fileDownload", null));
    }

    @Test
    public void simpleAclNimbusGroupsAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        clusterConf.put(Config.NIMBUS_ADMINS_GROUPS, Arrays.asList("admin-group"));
        clusterConf.put(Config.NIMBUS_SUPERVISOR_USERS, Arrays.asList("supervisor"));
        clusterConf.put(Config.NIMBUS_USERS, Arrays.asList("user-a"));
        clusterConf.put(Config.STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN, FixedGroupsMapping.class.getName());
        Map<String, Object> groups = new HashMap<>();
        groups.put("admin", Collections.singleton("admin-group"));
        groups.put("not-admin", Collections.singleton("not-admin-group"));
        Map<String, Object> groupsParams = new HashMap<>();
        groupsParams.put(FixedGroupsMapping.STORM_FIXED_GROUP_MAPPING, groups);
        clusterConf.put(Config.STORM_GROUP_MAPPING_SERVICE_PARAMS, groupsParams);

        ReqContext admin = new ReqContext(mkSubject("admin"));
        ReqContext notAdmin = new ReqContext(mkSubject("not-admin"));
        ReqContext supervisor = new ReqContext(mkSubject("supervisor"));
        ReqContext userA = new ReqContext(mkSubject("user-a"));
        ReqContext userB = new ReqContext(mkSubject("user-b"));

        final Map<String, Object> empty = Collections.emptyMap();

        SimpleACLAuthorizer authorizer = new SimpleACLAuthorizer();
        authorizer.prepare(clusterConf);

        assertTrue(authorizer.permit(userA, "submitTopology", empty));
        assertFalse(authorizer.permit(userB, "submitTopology", empty));

        assertTrue(authorizer.permit(admin, "fileUpload", null));
        assertFalse(authorizer.permit(notAdmin, "fileUpload", null));
        assertFalse(authorizer.permit(userB, "fileUpload", null));

        assertTrue(authorizer.permit(supervisor, "fileDownload", null));
    }

    @Test
    public void simpleAclSameUserAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        clusterConf.put(Config.NIMBUS_ADMINS, Arrays.asList("admin"));
        clusterConf.put(Config.NIMBUS_SUPERVISOR_USERS, Arrays.asList("admin"));
        ReqContext admin = new ReqContext(mkSubject("admin"));

        final Map<String, Object> empty = Collections.emptyMap();
        final Map<String, Object> aAllowed = new HashMap<>();
        aAllowed.put(Config.TOPOLOGY_USERS, Arrays.asList("user-a"));

        SimpleACLAuthorizer authorizer = new SimpleACLAuthorizer();
        authorizer.prepare(clusterConf);

        assertTrue(authorizer.permit(admin, "submitTopology", empty));
        assertTrue(authorizer.permit(admin, "fileUpload", null));
        assertTrue(authorizer.permit(admin, "getNimbusConf", null));
        assertTrue(authorizer.permit(admin, "getClusterInfo", null));
        assertTrue(authorizer.permit(admin, "fileDownload", null));
        assertTrue(authorizer.permit(admin, "killTopology", aAllowed));
        assertTrue(authorizer.permit(admin, "uploadNewCredentials", aAllowed));
        assertTrue(authorizer.permit(admin, "rebalance", aAllowed));
        assertTrue(authorizer.permit(admin, "activate", aAllowed));
        assertTrue(authorizer.permit(admin, "getTopologyConf", aAllowed));
        assertTrue(authorizer.permit(admin, "getTopology", aAllowed));
        assertTrue(authorizer.permit(admin, "getUserTopology", aAllowed));
        assertTrue(authorizer.permit(admin, "getTopologyInfo", aAllowed));
    }

    @Test
    public void shellBaseGroupsMappingTest() throws Exception {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        ShellBasedGroupsMapping groups = new ShellBasedGroupsMapping();
        groups.prepare(clusterConf);

        String userName = System.getProperty("user.name");

        assertTrue(groups.getGroups(userName).size() >= 0);
        assertEquals(0, groups.getGroups("userDoesNotExist").size());
        assertEquals(0, groups.getGroups(null).size());
    }

    @Test(expected = RuntimeException.class)
    public void getTransportPluginThrowsRunimeTest() {
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, "null.invalid");
        ClientAuthUtils.getTransportPlugin(ThriftConnectionType.NIMBUS, conf);
    }

    @Test
    public void impersonationAuthorizerTest() throws Exception {
        final String impersonatingUser = "admin";
        final String userBeingImpersonated = System.getProperty("user.name");
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        ShellBasedGroupsMapping groupMapper = new ShellBasedGroupsMapping();
        groupMapper.prepare(clusterConf);
        Set<String> groups = groupMapper.getGroups(userBeingImpersonated);

        InetAddress localHost = InetAddress.getLocalHost();

        Map<String, Object> acl = new HashMap<>();
        Map<String, Object> aclConf = new HashMap<>();
        aclConf.put("hosts", Arrays.asList(localHost.getHostName()));
        aclConf.put("groups", groups);
        acl.put(impersonatingUser, aclConf);
        clusterConf.put(Config.NIMBUS_IMPERSONATION_ACL, acl);

        InetAddress unauthorizedHost = com.google.common.net.InetAddresses.forString("10.10.10.10");

        ImpersonationAuthorizer authorizer = new ImpersonationAuthorizer();
        authorizer.prepare(clusterConf);

        //non impersonating request, should be permitted.
        assertTrue(authorizer.permit(new ReqContext(mkSubject("anyuser")), "fileUplaod", null));

        //user with no impersonation acl should be reject
        assertFalse(authorizer.permit(mkImpersonatingReqContext("user-with-no-acl", userBeingImpersonated, localHost),
                                      "someOperation", null));

        //request from hosts that are not authorized should be rejected
        assertFalse(authorizer.permit(mkImpersonatingReqContext(impersonatingUser, userBeingImpersonated, unauthorizedHost),
                                      "someOperation", null));

        //request to impersonate users from unauthroized groups should be rejected.
        assertFalse(authorizer.permit(mkImpersonatingReqContext(impersonatingUser, "unauthorized-user", localHost),
                                      "someOperation", null));

        //request from authorized hosts and group should be allowed.
        assertTrue(authorizer.permit(mkImpersonatingReqContext(impersonatingUser, userBeingImpersonated, localHost),
                                     "someOperation", null));
    }

    public interface MyBiConsumer<T, U> {
        void accept(T t, U u) throws Exception;
    }
}