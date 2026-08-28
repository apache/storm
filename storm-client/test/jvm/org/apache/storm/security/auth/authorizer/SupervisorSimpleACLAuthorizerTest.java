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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.utils.ConfigUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class SupervisorSimpleACLAuthorizerTest {

    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void requestWithoutPrincipalIsDenied() {
        IAuthorizer authorizer = new SupervisorSimpleACLAuthorizer();
        authorizer.prepare(ConfigUtils.readStormConfig());

        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_USERS, new HashSet<>(Collections.singletonList("user-a")));

        assertFalse(authorizer.permit(new ReqContext(new Subject()), "getLocalAssignmentForStorm", topoConf));
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void nimbusUserIsStillAllowedItsCommands() {
        Map<String, Object> clusterConf = ConfigUtils.readStormConfig();
        clusterConf.put(Config.NIMBUS_DAEMON_USERS, new HashSet<>(Collections.singletonList("nimbus-daemon")));

        IAuthorizer authorizer = new SupervisorSimpleACLAuthorizer();
        authorizer.prepare(clusterConf);

        assertTrue(authorizer.permit(new ReqContext(subjectOf("nimbus-daemon")),
            "sendSupervisorAssignments", new HashMap<>()));
    }

    private Subject subjectOf(String name) {
        Set<Principal> principals = new HashSet<>();
        principals.add(() -> name);
        return new Subject(true, principals, new HashSet<>(), new HashSet<>());
    }
}
