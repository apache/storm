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

package org.apache.storm.security.auth.authorizer;

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.utils.ConfigUtils;
import org.junit.Assert;
import org.junit.Test;

public class SimpleACLAuthorizerTest {

    @Test
    public void SimpleACLUserAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        Collection<String> adminUserSet = new HashSet<>(Arrays.asList("admin"));
        Collection<String> supervisorUserSet = new HashSet<>(Arrays.asList("supervisor"));
        clusterConf.put(Config.NIMBUS_ADMINS, adminUserSet);
        clusterConf.put(Config.NIMBUS_SUPERVISOR_USERS, supervisorUserSet);

        IAuthorizer authorizer = new SimpleACLAuthorizer();

        Subject adminUser = createSubject("admin");
        Subject supervisorUser = createSubject("supervisor");
        Subject userA = createSubject("user-a");
        Subject userB = createSubject("user-b");

        authorizer.prepare(clusterConf);

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "submitTopology", new HashMap<>()));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "submitTopology", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "submitTopology", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(userB), "submitTopology", new HashMap<>()));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "fileUpload", new HashMap<>()));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "fileUpload", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "fileUpload", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(userB), "fileUpload", new HashMap<>()));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getNimbusConf", new HashMap<>()));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getNimbusConf", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getNimbusConf", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(userB), "getNimbusConf", new HashMap<>()));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getClusterInfo", new HashMap<>()));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getClusterInfo", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getClusterInfo", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(userB), "getClusterInfo", new HashMap<>()));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getSupervisorPageInfo", new HashMap<>()));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getSupervisorPageInfo", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getSupervisorPageInfo", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(userB), "getSupervisorPageInfo", new HashMap<>()));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "fileDownload", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(supervisorUser), "fileDownload", new HashMap<>()));
        Assert.assertFalse(authorizer.permit(new ReqContext(userA), "fileDownload", new HashMap<>()));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "fileDownload", new HashMap<>()));

        Map<String, Object> topoConf = new HashMap<>();
        Collection<String> topologyUserSet = new HashSet<>(Arrays.asList("user-a"));
        topoConf.put(Config.TOPOLOGY_USERS, topologyUserSet);

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "killTopology", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "killTopology", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "killTopology", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "killTopology", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "rebalance", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "rebalance", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "rebalance", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "rebalance", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "activate", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "activate", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "activate", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "activate", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "deactivate", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "deactivate", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "deactivate", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "deactivate", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getTopologyConf", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getTopologyConf", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyConf", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyConf", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getTopology", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getTopology", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getTopology", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getTopology", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getUserTopology", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getUserTopology", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getUserTopology", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getUserTopology", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getTopologyInfo", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getTopologyInfo", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyInfo", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyInfo", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getTopologyPageInfo", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getTopologyPageInfo", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyPageInfo", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyPageInfo", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getComponentPageInfo", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getComponentPageInfo", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getComponentPageInfo", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getComponentPageInfo", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "uploadNewCredentials", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "uploadNewCredentials", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "uploadNewCredentials", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "uploadNewCredentials", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "setLogConfig", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "setLogConfig", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "setLogConfig", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "setLogConfig", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "setWorkerProfiler", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "setWorkerProfiler", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "setWorkerProfiler", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "setWorkerProfiler", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getWorkerProfileActionExpiry", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getWorkerProfileActionExpiry", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getWorkerProfileActionExpiry", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getWorkerProfileActionExpiry", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getComponentPendingProfileActions", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getComponentPendingProfileActions", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getComponentPendingProfileActions", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getComponentPendingProfileActions", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "startProfiling", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "startProfiling", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "startProfiling", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "startProfiling", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "stopProfiling", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "stopProfiling", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "stopProfiling", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "stopProfiling", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "dumpProfile", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "dumpProfile", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "dumpProfile", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "dumpProfile", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "dumpJstack", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "dumpJstack", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "dumpJstack", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "dumpJstack", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "dumpHeap", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "dumpHeap", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "dumpHeap", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "dumpHeap", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "debug", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "debug", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "debug", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "debug", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "getLogConfig", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getLogConfig", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getLogConfig", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getLogConfig", topoConf));
    }

    @Test
    public void SimpleACLNimbusUserAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        Collection<String> adminUserSet = new HashSet<>(Arrays.asList("admin"));
        Collection<String> supervisorUserSet = new HashSet<>(Arrays.asList("supervisor"));
        Collection<String> nimbusUserSet = new HashSet<>(Arrays.asList("user-a"));

        clusterConf.put(Config.NIMBUS_ADMINS, adminUserSet);
        clusterConf.put(Config.NIMBUS_SUPERVISOR_USERS, supervisorUserSet);
        clusterConf.put(Config.NIMBUS_USERS, nimbusUserSet);

        IAuthorizer authorizer = new SimpleACLAuthorizer();

        Subject adminUser = createSubject("admin");
        Subject supervisorUser = createSubject("supervisor");
        Subject userA = createSubject("user-a");
        Subject userB = createSubject("user-b");

        authorizer.prepare(clusterConf);

        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "submitTopology", new HashMap<>()));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "submitTopology", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(adminUser), "fileUpload", new HashMap<>()));
        Assert.assertTrue(authorizer.permit(new ReqContext(supervisorUser), "fileDownload", new HashMap<>()));
    }

    @Test
    public void SimpleACLTopologyReadOnlyUserAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();

        Map<String, Object> topoConf = new HashMap<>();
        Collection<String> topologyUserSet = new HashSet<>(Arrays.asList("user-a"));
        topoConf.put(Config.TOPOLOGY_USERS, topologyUserSet);

        Collection<String> topologyReadOnlyUserSet = new HashSet<>(Arrays.asList("user-readonly"));
        topoConf.put(Config.TOPOLOGY_READONLY_USERS, topologyReadOnlyUserSet);

        Subject userA = createSubject("user-a");
        Subject userB = createSubject("user-b");
        Subject readOnlyUser = createSubject("user-readonly");

        IAuthorizer authorizer = new SimpleACLAuthorizer();
        authorizer.prepare(clusterConf);

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "killTopology", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "killTopology", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "killTopology", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "rebalance", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "rebalance", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "rebalance", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "activate", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "activate", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "activate", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "deactivate", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "deactivate", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "deactivate", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getTopologyConf", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyConf", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyConf", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getTopology", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getTopology", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getTopology", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getUserTopology", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getUserTopology", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getUserTopology", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getTopologyInfo", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyInfo", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyInfo", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getTopologyPageInfo", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyPageInfo", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyPageInfo", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getComponentPageInfo", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getComponentPageInfo", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getComponentPageInfo", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "uploadNewCredentials", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "uploadNewCredentials", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "uploadNewCredentials", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "setLogConfig", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "setLogConfig", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "setLogConfig", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "setWorkerProfiler", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "setWorkerProfiler", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "setWorkerProfiler", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getWorkerProfileActionExpiry", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getWorkerProfileActionExpiry", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getWorkerProfileActionExpiry", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getComponentPendingProfileActions", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getComponentPendingProfileActions", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getComponentPendingProfileActions", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "startProfiling", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "startProfiling", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "startProfiling", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "stopProfiling", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "stopProfiling", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "stopProfiling", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "dumpProfile", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "dumpProfile", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "dumpProfile", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "dumpJstack", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "dumpJstack", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "dumpJstack", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "dumpHeap", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "dumpHeap", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "dumpHeap", topoConf));

        Assert.assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "debug", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "debug", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "debug", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getLogConfig", topoConf));
        Assert.assertTrue(authorizer.permit(new ReqContext(userA), "getLogConfig", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getLogConfig", topoConf));
    }

    @Test
    public void SimpleACLTopologyReadOnlyGroupAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        clusterConf.put(Config.STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN,
                        SimpleACLTopologyReadOnlyGroupAuthTestMock.class.getName());

        Map<String, Object> topoConf = new HashMap<>();
        Collection<String> topologyReadOnlyGroupSet = new HashSet<>(Arrays.asList("group-readonly"));
        topoConf.put(Config.TOPOLOGY_READONLY_GROUPS, topologyReadOnlyGroupSet);

        Subject userInReadOnlyGroup = createSubject("user-in-readonly-group");
        Subject userB = createSubject("user-b");

        IAuthorizer authorizer = new SimpleACLAuthorizer();
        authorizer.prepare(clusterConf);

        Assert.assertFalse(authorizer.permit(new ReqContext(userInReadOnlyGroup), "killTopology", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "killTopology", topoConf));

        Assert.assertTrue(authorizer.permit(new ReqContext(userInReadOnlyGroup), "getTopologyInfo", topoConf));
        Assert.assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyInfo", topoConf));
    }

    private Subject createSubject(String name) {
        Set<Principal> principalSet = new HashSet<>();
        principalSet.add(createPrincipal(name));
        return new Subject(true, principalSet, new HashSet(), new HashSet());
    }

    private Principal createPrincipal(String name) {
        return new Principal() {
            @Override
            public String getName() {
                return name;
            }
        };
    }

    public static class SimpleACLTopologyReadOnlyGroupAuthTestMock implements IGroupMappingServiceProvider {

        @Override
        public void prepare(Map<String, Object> conf) {
            //Ignored
        }

        @Override
        public Set<String> getGroups(String user) throws IOException {
            if ("user-in-readonly-group".equals(user)) {
                return new HashSet<>(Arrays.asList("group-readonly"));
            } else {
                return new HashSet<>(Arrays.asList(user));
            }
        }
    }
}