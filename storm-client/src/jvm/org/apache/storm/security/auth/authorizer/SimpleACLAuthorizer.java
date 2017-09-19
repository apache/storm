/**
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

package org.apache.storm.security.auth.authorizer;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IPrincipalToLocal;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authorization implementation that simply checks if a user is allowed to perform specific
 * operations.
 */
public class SimpleACLAuthorizer implements IAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleACLAuthorizer.class);

    protected Set<String> _userCommands = new HashSet<>(Arrays.asList(
            "submitTopology",
            "fileUpload",
            "getNimbusConf",
            "getClusterInfo",
            "getSupervisorPageInfo",
            "getOwnerResourceSummaries"));
    protected Set<String> _supervisorCommands = new HashSet<>(Arrays.asList("fileDownload"));
    protected Set<String> _topoReadOnlyCommands = new HashSet<>(Arrays.asList(
            "getTopologyConf",
            "getTopology",
            "getUserTopology",
            "getTopologyInfo",
            "getTopologyPageInfo",
            "getComponentPageInfo",
            "getWorkerProfileActionExpiry",
            "getComponentPendingProfileActions",
            "getLogConfig"));
    protected Set<String> _topoCommands = new HashSet<>(Arrays.asList(
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
            "debug"));

    {
        _topoCommands.addAll(_topoReadOnlyCommands);
    }

    protected Set<String> _admins;
    protected Set<String> _supervisors;
    protected Set<String> _nimbusUsers;
    protected Set<String> _nimbusGroups;
    protected IPrincipalToLocal _ptol;
    protected IGroupMappingServiceProvider _groupMappingProvider;
    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration
     */
    @Override
    public void prepare(Map<String, Object> conf) {
        _admins = new HashSet<>();
        _supervisors = new HashSet<>();
        _nimbusUsers = new HashSet<>();
        _nimbusGroups = new HashSet<>();

        if (conf.containsKey(Config.NIMBUS_ADMINS)) {
            _admins.addAll((Collection<String>)conf.get(Config.NIMBUS_ADMINS));
        }
        if (conf.containsKey(Config.NIMBUS_SUPERVISOR_USERS)) {
            _supervisors.addAll((Collection<String>)conf.get(Config.NIMBUS_SUPERVISOR_USERS));
        }
        if (conf.containsKey(Config.NIMBUS_USERS)) {
            _nimbusUsers.addAll((Collection<String>)conf.get(Config.NIMBUS_USERS));
        }

        if (conf.containsKey(Config.NIMBUS_GROUPS)) {
            _nimbusGroups.addAll((Collection<String>)conf.get(Config.NIMBUS_GROUPS));
        }

        _ptol = AuthUtils.GetPrincipalToLocalPlugin(conf);
        _groupMappingProvider = AuthUtils.GetGroupMappingServiceProviderPlugin(conf);
    }

    /**
     * permit() method is invoked for each incoming Thrift request
     * @param context request context includes info about
     * @param operation operation name
     * @param topoConf configuration of targeted topology
     * @return true if the request is authorized, false if reject
     */
    @Override
    public boolean permit(ReqContext context, String operation, Map<String, Object> topoConf) {
        String principal = context.principal().getName();
        String user = _ptol.toLocal(context.principal());
        Set<String> userGroups = new HashSet<>();

        if (_groupMappingProvider != null) {
            try {
                userGroups = _groupMappingProvider.getGroups(user);
            } catch(IOException e) {
                LOG.warn("Error while trying to fetch user groups",e);
            }
        }

        if (_admins.contains(principal) || _admins.contains(user)) {
            return true;
        }

        if (_supervisors.contains(principal) || _supervisors.contains(user)) {
            return _supervisorCommands.contains(operation);
        }

        if (_userCommands.contains(operation)) {
            return _nimbusUsers.size() == 0 || _nimbusUsers.contains(user) || checkUserGroupAllowed(userGroups, _nimbusGroups);
        }

        if (_topoCommands.contains(operation)) {
            if (checkTopoPermission(principal, user, userGroups, topoConf, Config.TOPOLOGY_USERS, Config.TOPOLOGY_GROUPS)) {
                return true;
            }

            if (_topoReadOnlyCommands.contains(operation) && checkTopoPermission(principal, user, userGroups,
                    topoConf, Config.TOPOLOGY_READONLY_USERS, Config.TOPOLOGY_READONLY_GROUPS)) {
                return true;
            }
        }
        return false;
    }

    private Boolean checkTopoPermission(String principal, String user, Set<String> userGroups,
                                        Map<String, Object> topoConf, String userConfigKey, String groupConfigKey){
        Set<String> configuredUsers = new HashSet<>();

        if (topoConf.containsKey(userConfigKey)) {
            configuredUsers.addAll((Collection<String>)topoConf.get(userConfigKey));
        }

        if (configuredUsers.contains(principal) || configuredUsers.contains(user)) {
            return true;
        }

        Set<String> configuredGroups = new HashSet<>();
        if (topoConf.containsKey(groupConfigKey) && topoConf.get(groupConfigKey) != null) {
            configuredGroups.addAll((Collection<String>)topoConf.get(groupConfigKey));
        }

        return checkUserGroupAllowed(userGroups, configuredGroups);
    }

    private Boolean checkUserGroupAllowed(Set<String> userGroups, Set<String> configuredGroups) {
        if(userGroups.size() > 0 && configuredGroups.size() > 0) {
            for (String tgroup : configuredGroups) {
                if(userGroups.contains(tgroup))
                    return true;
            }
        }
        return false;
    }
}
