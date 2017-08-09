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

package org.apache.storm.daemon.logviewer.utils;

import static org.apache.storm.DaemonConfig.UI_FILTER;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.security.auth.IPrincipalToLocal;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.utils.Utils;

public class ResourceAuthorizer {

    private final Map<String, Object> stormConf;
    private final IGroupMappingServiceProvider groupMappingServiceProvider;
    private final IPrincipalToLocal principalToLocal;

    /**
     * Constuctor.
     *
     * @param stormConf storm configuration
     */
    public ResourceAuthorizer(Map<String, Object> stormConf) {
        this.stormConf = stormConf;
        this.groupMappingServiceProvider = AuthUtils.GetGroupMappingServiceProviderPlugin(stormConf);
        this.principalToLocal = AuthUtils.GetPrincipalToLocalPlugin(stormConf);
    }

    /**
     * Checks whether user is allowed to access file via UI. Always true when UI filter is not set.
     *
     * @param fileName file name to access
     * @param user username
     */
    public boolean isUserAllowedToAccessFile(String user, String fileName) {
        return isUiFilterNotSet() || isAuthorizedLogUser(user, fileName);
    }

    /**
     * Checks whether user is authorized to access file. Checks regardless of UI filter.
     *
     * @param user username
     * @param fileName file name to access
     */
    public boolean isAuthorizedLogUser(String user, String fileName) {
        if (StringUtils.isEmpty(user) || StringUtils.isEmpty(fileName)
                || getLogUserGroupWhitelist(fileName) == null) {
            return false;
        } else {
            LogUserGroupWhitelist whitelist = getLogUserGroupWhitelist(fileName);

            List<String> logsUsers = new ArrayList<>();
            logsUsers.addAll(ObjectReader.getStrings(stormConf.get(DaemonConfig.LOGS_USERS)));
            logsUsers.addAll(ObjectReader.getStrings(stormConf.get(Config.NIMBUS_ADMINS)));
            logsUsers.addAll(whitelist.getUserWhitelist());

            List<String> logsGroups = new ArrayList<>();
            logsGroups.addAll(ObjectReader.getStrings(stormConf.get(DaemonConfig.LOGS_GROUPS)));
            logsGroups.addAll(whitelist.getGroupWhitelist());

            String userName = principalToLocal.toLocal(user);
            Set<String> groups = getUserGroups(userName);

            return logsUsers.stream().anyMatch(u -> u.equals(userName))
                    || Sets.intersection(groups, new HashSet<>(logsGroups)).size() > 0;
        }
    }

    /**
     * Get the whitelist of users and groups for given file.
     *
     * @param fileName file name to get the whitelist
     */
    public LogUserGroupWhitelist getLogUserGroupWhitelist(String fileName) {
        File wlFile = ServerConfigUtils.getLogMetaDataFile(fileName);
        Map<String, Object> map = (Map<String, Object>) Utils.readYamlFile(wlFile.getAbsolutePath());

        if (map == null) {
            return null;
        }

        List<String> logsUsers = ObjectReader.getStrings(map.get(DaemonConfig.LOGS_USERS));
        List<String> logsGroups = ObjectReader.getStrings(map.get(DaemonConfig.LOGS_GROUPS));
        return new LogUserGroupWhitelist(
                logsUsers.isEmpty() ? new HashSet<>() : new HashSet<>(logsUsers),
                logsGroups.isEmpty() ? new HashSet<>() : new HashSet<>(logsGroups)
        );
    }

    @VisibleForTesting
    Set<String> getUserGroups(String user) {
        try {
            if (StringUtils.isEmpty(user)) {
                return new HashSet<>();
            } else {
                return groupMappingServiceProvider.getGroups(user);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isUiFilterNotSet() {
        return StringUtils.isBlank(ObjectReader.getString(stormConf.get(UI_FILTER), null));
    }

    public static class LogUserGroupWhitelist {

        private Set<String> userWhitelist;
        private Set<String> groupWhitelist;

        public LogUserGroupWhitelist(Set<String> userWhitelist, Set<String> groupWhitelist) {
            this.userWhitelist = userWhitelist;
            this.groupWhitelist = groupWhitelist;
        }

        public Set<String> getUserWhitelist() {
            return userWhitelist;
        }

        public Set<String> getGroupWhitelist() {
            return groupWhitelist;
        }

    }
}
