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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.security.auth.IPrincipalToLocal;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authorization implementation that simply checks if a user is allowed to perform specific operations.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class SimpleACLAuthorizer implements IAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleACLAuthorizer.class);

    protected Set<String> userCommands = new HashSet<>(Arrays.asList(
        "submitTopology",
        "fileUpload",
        "getNimbusConf",
        "getClusterInfo",
        "getLeader",
        "isTopologyNameAllowed",
        "getTopologySummaries",
        "getTopologySummaryByName",
        "getTopologySummary",
        "getSupervisorPageInfo",
        "getOwnerResourceSummaries"));
    protected Set<String> supervisorCommands = new HashSet<>(Arrays.asList(
        "fileDownload",
        "processWorkerMetrics",
        "getSupervisorAssignments",
        "sendSupervisorWorkerHeartbeats"));
    protected Set<String> topoReadOnlyCommands = new HashSet<>(Arrays.asList(
        "getTopologyConf",
        "getTopology",
        "getUserTopology",
        "getTopologyInfo",
        "getTopologyPageInfo",
        "getComponentPageInfo",
        "getWorkerProfileActionExpiry",
        "getComponentPendingProfileActions",
        "getLogConfig"));
    protected Set<String> topoCommands = new HashSet<>(Arrays.asList(
        "killTopology",
        "rebalance",
        "activate",
        "deactivate",
        "uploadNewCredentials",
        "setLogConfig",
        "setWorkerProfiler",
        "startProfiling",
        "stopProfiling",
        "dumpProfile",
        "dumpJstack",
        "dumpHeap",
        "debug",
        "sendSupervisorWorkerHeartbeat"));
    protected Set<String> admins;
    protected Set<String> adminsGroups;
    protected Set<String> supervisors;
    protected Set<String> nimbusUsers;
    protected Set<String> nimbusGroups;
    protected IPrincipalToLocal ptol;
    protected IGroupMappingServiceProvider groupMappingServiceProvider;

    {
        topoCommands.addAll(topoReadOnlyCommands);
    }

    /**
     * Invoked once immediately after construction.
     *
     * @param conf Storm configuration
     */
    @Override
    public void prepare(Map<String, Object> conf) {
        admins = new HashSet<>();
        adminsGroups = new HashSet<>();
        supervisors = new HashSet<>();
        nimbusUsers = new HashSet<>();
        nimbusGroups = new HashSet<>();

        if (conf.containsKey(Config.NIMBUS_ADMINS)) {
            admins.addAll((Collection<String>) conf.get(Config.NIMBUS_ADMINS));
        }

        if (conf.containsKey(Config.NIMBUS_ADMINS_GROUPS)) {
            adminsGroups.addAll((Collection<String>) conf.get(Config.NIMBUS_ADMINS_GROUPS));
        }

        if (conf.containsKey(Config.NIMBUS_SUPERVISOR_USERS)) {
            supervisors.addAll((Collection<String>) conf.get(Config.NIMBUS_SUPERVISOR_USERS));
        }

        if (conf.containsKey(Config.NIMBUS_USERS)) {
            nimbusUsers.addAll((Collection<String>) conf.get(Config.NIMBUS_USERS));
        }

        if (conf.containsKey(Config.NIMBUS_GROUPS)) {
            nimbusGroups.addAll((Collection<String>) conf.get(Config.NIMBUS_GROUPS));
        }

        ptol = ClientAuthUtils.getPrincipalToLocalPlugin(conf);
        groupMappingServiceProvider = ClientAuthUtils.getGroupMappingServiceProviderPlugin(conf);
    }

    /**
     * permit() method is invoked for each incoming Thrift request.
     *
     * @param context   request context includes info about
     * @param operation operation name
     * @param topoConf  configuration of targeted topology
     * @return true if the request is authorized, false if reject
     */
    @Override
    public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
        String principal = context.principal().getName();
        String user = ptol.toLocal(context.principal());
        Set<String> userGroups = new HashSet<>();

        if (groupMappingServiceProvider != null) {
            try {
                userGroups = groupMappingServiceProvider.getGroups(user);
            } catch (IOException e) {
                LOG.warn("Error while trying to fetch user groups", e);
            }
        }

        if (admins.contains(principal) || admins.contains(user) || checkUserGroupAllowed(userGroups, adminsGroups)) {
            return true;
        }

        if (supervisors.contains(principal) || supervisors.contains(user)) {
            return supervisorCommands.contains(operation);
        }

        if (userCommands.contains(operation)) {
            return nimbusUsers.size() == 0 || nimbusUsers.contains(user) || checkUserGroupAllowed(userGroups, nimbusGroups);
        }

        if (topoCommands.contains(operation)) {
            if (checkTopoPermission(principal, user, userGroups, topoConf, Config.TOPOLOGY_USERS, Config.TOPOLOGY_GROUPS)) {
                return true;
            }

            if (topoReadOnlyCommands.contains(operation) && checkTopoPermission(principal, user, userGroups,
                                                                                topoConf, Config.TOPOLOGY_READONLY_USERS,
                                                                                Config.TOPOLOGY_READONLY_GROUPS)) {
                return true;
            }
        }
        return false;
    }

    private Boolean checkTopoPermission(String principal, String user, Set<String> userGroups,
                                        Map<String, Object> topoConf, String userConfigKey, String groupConfigKey) {
        Set<String> configuredUsers = new HashSet<>();

        if (topoConf.containsKey(userConfigKey)) {
            configuredUsers.addAll(ObjectReader.getStrings(topoConf.get(userConfigKey)));
        }

        if (configuredUsers.contains(principal) || configuredUsers.contains(user)) {
            return true;
        }

        Set<String> configuredGroups = new HashSet<>();
        if (topoConf.containsKey(groupConfigKey)) {
            configuredGroups.addAll(ObjectReader.getStrings(topoConf.get(groupConfigKey)));
        }

        return checkUserGroupAllowed(userGroups, configuredGroups);
    }

    private Boolean checkUserGroupAllowed(Set<String> userGroups, Set<String> configuredGroups) {
        if (userGroups.size() > 0 && configuredGroups.size() > 0) {
            for (String tgroup : configuredGroups) {
                if (userGroups.contains(tgroup)) {
                    return true;
                }
            }
        }
        return false;
    }
}
