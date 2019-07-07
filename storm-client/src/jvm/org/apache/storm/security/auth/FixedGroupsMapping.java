/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.security.auth;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FixedGroupsMapping implements IGroupMappingServiceProvider {

    public static final String STORM_FIXED_GROUP_MAPPING = "storm.fixed.group.mapping";
    public static Logger LOG = LoggerFactory.getLogger(FixedGroupsMapping.class);
    public Map<String, Set<String>> cachedGroups = new HashMap<String, Set<String>>();

    /**
     * Invoked once immediately after construction.
     *
     * @param stormConf Storm configuration
     */
    @Override
    public void prepare(Map<String, Object> stormConf) {
        Map<?, ?> params = (Map<?, ?>) stormConf.get(Config.STORM_GROUP_MAPPING_SERVICE_PARAMS);
        Map<String, Set<String>> mapping = (Map<String, Set<String>>) params.get(STORM_FIXED_GROUP_MAPPING);
        if (mapping != null) {
            cachedGroups.putAll(mapping);
        } else {
            LOG.warn("There is no initial group mapping");
        }
    }

    /**
     * Returns list of groups for a user.
     *
     * @param user get groups for this user
     * @return list of groups for a given user
     */
    @Override
    public Set<String> getGroups(String user) throws IOException {
        if (cachedGroups.containsKey(user)) {
            return cachedGroups.get(user);
        }

        // I don't have anything
        return new HashSet<String>();
    }
}
