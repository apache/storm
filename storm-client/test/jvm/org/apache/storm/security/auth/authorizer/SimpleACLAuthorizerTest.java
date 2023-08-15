/*
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

import org.apache.storm.Config;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.utils.ConfigUtils;
import org.junit.jupiter.api.Test;

import javax.security.auth.Subject;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleACLAuthorizerTest {

    @Test
    public void SimpleACLUserAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        Collection<String> adminUserSet = new HashSet<>(Collections.singletonList("admin"));
        Collection<String> supervisorUserSet = new HashSet<>(Collections.singletonList("supervisor"));
        clusterConf.put(Config.NIMBUS_ADMINS, adminUserSet);
        clusterConf.put(Config.NIMBUS_SUPERVISOR_USERS, supervisorUserSet);

        IAuthorizer authorizer = new SimpleACLAuthorizer();

        Subject adminUser = createSubject("admin");
        Subject supervisorUser = createSubject("supervisor");
        Subject userA = createSubject("user-a");
        Subject userB = createSubject("user-b");

        authorizer.prepare(clusterConf);

        assertTrue(authorizer.permit(new ReqContext(adminUser), "submitTopology", new HashMap<>()));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "submitTopology", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(userA), "submitTopology", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(userB), "submitTopology", new HashMap<>()));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "fileUpload", new HashMap<>()));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "fileUpload", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(userA), "fileUpload", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(userB), "fileUpload", new HashMap<>()));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getNimbusConf", new HashMap<>()));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getNimbusConf", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(userA), "getNimbusConf", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(userB), "getNimbusConf", new HashMap<>()));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getClusterInfo", new HashMap<>()));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getClusterInfo", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(userA), "getClusterInfo", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(userB), "getClusterInfo", new HashMap<>()));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getSupervisorPageInfo", new HashMap<>()));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getSupervisorPageInfo", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(userA), "getSupervisorPageInfo", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(userB), "getSupervisorPageInfo", new HashMap<>()));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "fileDownload", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(supervisorUser), "fileDownload", new HashMap<>()));
        assertFalse(authorizer.permit(new ReqContext(userA), "fileDownload", new HashMap<>()));
        assertFalse(authorizer.permit(new ReqContext(userB), "fileDownload", new HashMap<>()));

        Map<String, Object> topoConf = new HashMap<>();
        Collection<String> topologyUserSet = new HashSet<>(Collections.singletonList("user-a"));
        topoConf.put(Config.TOPOLOGY_USERS, topologyUserSet);

        assertTrue(authorizer.permit(new ReqContext(adminUser), "killTopology", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "killTopology", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "killTopology", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "killTopology", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "rebalance", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "rebalance", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "rebalance", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "rebalance", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "activate", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "activate", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "activate", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "activate", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "deactivate", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "deactivate", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "deactivate", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "deactivate", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getTopologyConf", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getTopologyConf", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyConf", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyConf", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getTopology", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getTopology", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getTopology", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getTopology", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getUserTopology", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getUserTopology", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getUserTopology", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getUserTopology", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getTopologyInfo", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getTopologyInfo", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyInfo", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyInfo", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getTopologyPageInfo", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getTopologyPageInfo", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyPageInfo", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyPageInfo", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getComponentPageInfo", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getComponentPageInfo", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getComponentPageInfo", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getComponentPageInfo", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "uploadNewCredentials", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "uploadNewCredentials", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "uploadNewCredentials", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "uploadNewCredentials", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "setLogConfig", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "setLogConfig", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "setLogConfig", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "setLogConfig", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "setWorkerProfiler", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "setWorkerProfiler", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "setWorkerProfiler", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "setWorkerProfiler", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getWorkerProfileActionExpiry", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getWorkerProfileActionExpiry", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getWorkerProfileActionExpiry", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getWorkerProfileActionExpiry", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getComponentPendingProfileActions", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getComponentPendingProfileActions", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getComponentPendingProfileActions", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getComponentPendingProfileActions", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "startProfiling", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "startProfiling", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "startProfiling", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "startProfiling", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "stopProfiling", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "stopProfiling", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "stopProfiling", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "stopProfiling", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "dumpProfile", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "dumpProfile", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "dumpProfile", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "dumpProfile", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "dumpJstack", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "dumpJstack", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "dumpJstack", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "dumpJstack", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "dumpHeap", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "dumpHeap", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "dumpHeap", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "dumpHeap", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "debug", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "debug", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "debug", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "debug", topoConf));

        assertTrue(authorizer.permit(new ReqContext(adminUser), "getLogConfig", topoConf));
        assertFalse(authorizer.permit(new ReqContext(supervisorUser), "getLogConfig", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getLogConfig", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getLogConfig", topoConf));
    }

    @Test
    public void SimpleACLNimbusUserAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        Collection<String> adminUserSet = new HashSet<>(Collections.singletonList("admin"));
        Collection<String> supervisorUserSet = new HashSet<>(Collections.singletonList("supervisor"));
        Collection<String> nimbusUserSet = new HashSet<>(Collections.singletonList("user-a"));

        clusterConf.put(Config.NIMBUS_ADMINS, adminUserSet);
        clusterConf.put(Config.NIMBUS_SUPERVISOR_USERS, supervisorUserSet);
        clusterConf.put(Config.NIMBUS_USERS, nimbusUserSet);

        IAuthorizer authorizer = new SimpleACLAuthorizer();

        Subject adminUser = createSubject("admin");
        Subject supervisorUser = createSubject("supervisor");
        Subject userA = createSubject("user-a");
        Subject userB = createSubject("user-b");

        authorizer.prepare(clusterConf);

        assertTrue(authorizer.permit(new ReqContext(userA), "submitTopology", new HashMap<>()));
        assertFalse(authorizer.permit(new ReqContext(userB), "submitTopology", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(adminUser), "fileUpload", new HashMap<>()));
        assertTrue(authorizer.permit(new ReqContext(supervisorUser), "fileDownload", new HashMap<>()));
    }

    @Test
    public void SimpleACLTopologyReadOnlyUserAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();

        Map<String, Object> topoConf = new HashMap<>();
        Collection<String> topologyUserSet = new HashSet<>(Collections.singletonList("user-a"));
        topoConf.put(Config.TOPOLOGY_USERS, topologyUserSet);

        Collection<String> topologyReadOnlyUserSet = new HashSet<>(Collections.singletonList("user-readonly"));
        topoConf.put(Config.TOPOLOGY_READONLY_USERS, topologyReadOnlyUserSet);

        Subject userA = createSubject("user-a");
        Subject userB = createSubject("user-b");
        Subject readOnlyUser = createSubject("user-readonly");

        IAuthorizer authorizer = new SimpleACLAuthorizer();
        authorizer.prepare(clusterConf);

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "killTopology", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "killTopology", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "killTopology", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "rebalance", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "rebalance", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "rebalance", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "activate", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "activate", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "activate", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "deactivate", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "deactivate", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "deactivate", topoConf));

        assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getTopologyConf", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyConf", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyConf", topoConf));

        assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getTopology", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getTopology", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getTopology", topoConf));

        assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getUserTopology", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getUserTopology", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getUserTopology", topoConf));

        assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getTopologyInfo", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyInfo", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyInfo", topoConf));

        assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getTopologyPageInfo", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getTopologyPageInfo", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyPageInfo", topoConf));

        assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getComponentPageInfo", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getComponentPageInfo", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getComponentPageInfo", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "uploadNewCredentials", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "uploadNewCredentials", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "uploadNewCredentials", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "setLogConfig", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "setLogConfig", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "setLogConfig", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "setWorkerProfiler", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "setWorkerProfiler", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "setWorkerProfiler", topoConf));

        assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getWorkerProfileActionExpiry", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getWorkerProfileActionExpiry", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getWorkerProfileActionExpiry", topoConf));

        assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getComponentPendingProfileActions", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getComponentPendingProfileActions", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getComponentPendingProfileActions", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "startProfiling", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "startProfiling", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "startProfiling", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "stopProfiling", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "stopProfiling", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "stopProfiling", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "dumpProfile", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "dumpProfile", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "dumpProfile", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "dumpJstack", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "dumpJstack", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "dumpJstack", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "dumpHeap", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "dumpHeap", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "dumpHeap", topoConf));

        assertFalse(authorizer.permit(new ReqContext(readOnlyUser), "debug", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "debug", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "debug", topoConf));

        assertTrue(authorizer.permit(new ReqContext(readOnlyUser), "getLogConfig", topoConf));
        assertTrue(authorizer.permit(new ReqContext(userA), "getLogConfig", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getLogConfig", topoConf));
    }

    @Test
    public void SimpleACLTopologyReadOnlyGroupAuthTest() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        clusterConf.put(Config.STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN,
                        SimpleACLTopologyReadOnlyGroupAuthTestMock.class.getName());

        Map<String, Object> topoConf = new HashMap<>();
        Collection<String> topologyReadOnlyGroupSet = new HashSet<>(Collections.singletonList("group-readonly"));
        topoConf.put(Config.TOPOLOGY_READONLY_GROUPS, topologyReadOnlyGroupSet);

        Subject userInReadOnlyGroup = createSubject("user-in-readonly-group");
        Subject userB = createSubject("user-b");

        IAuthorizer authorizer = new SimpleACLAuthorizer();
        authorizer.prepare(clusterConf);

        assertFalse(authorizer.permit(new ReqContext(userInReadOnlyGroup), "killTopology", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "killTopology", topoConf));

        assertTrue(authorizer.permit(new ReqContext(userInReadOnlyGroup), "getTopologyInfo", topoConf));
        assertFalse(authorizer.permit(new ReqContext(userB), "getTopologyInfo", topoConf));
    }

    private Subject createSubject(String name) {
        Set<Principal> principalSet = new HashSet<>();
        principalSet.add(createPrincipal(name));
        return new Subject(true, principalSet, new HashSet<>(), new HashSet<>());
    }

    private Principal createPrincipal(String name) {
        return () -> name;
    }

    public static class SimpleACLTopologyReadOnlyGroupAuthTestMock implements IGroupMappingServiceProvider {

        @Override
        public void prepare(Map<String, Object> conf) {
            //Ignored
        }

        @Override
        public Set<String> getGroups(String user) {
            if ("user-in-readonly-group".equals(user)) {
                return new HashSet<>(Collections.singletonList("group-readonly"));
            } else {
                return new HashSet<>(Collections.singletonList(user));
            }
        }
    }
}