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

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.ShellCommandRunner;
import org.apache.storm.utils.ShellCommandRunnerImpl;
import org.apache.storm.utils.ShellUtils;
import org.apache.storm.utils.ShellUtils.ExitCodeException;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ShellBasedGroupsMapping implements
                                     IGroupMappingServiceProvider {

    public static final Logger LOG = LoggerFactory.getLogger(ShellBasedGroupsMapping.class);
    private final ShellCommandRunner shellCommandRunner;
    public RotatingMap<String, Set<String>> cachedGroups;
    private long timeoutMs;
    private volatile long lastRotationMs;

    public ShellBasedGroupsMapping() {
        this(new ShellCommandRunnerImpl());
    }

    @VisibleForTesting
    ShellBasedGroupsMapping(ShellCommandRunner shellCommandRunner) {
        this.shellCommandRunner = shellCommandRunner;
    }

    /**
     * Invoked once immediately after construction.
     *
     * @param topoConf Storm configuration
     */
    @Override
    public void prepare(Map<String, Object> topoConf) {
        timeoutMs = TimeUnit.SECONDS.toMillis(ObjectReader.getInt(topoConf.get(Config.STORM_GROUP_MAPPING_SERVICE_CACHE_DURATION_SECS)));
        lastRotationMs = Time.currentTimeMillis();
        cachedGroups = new RotatingMap<>(2);
    }

    /**
     * Returns list of groups for a user.
     *
     * @param user get groups for this user
     * @return list of groups for a given user
     */
    @Override
    public Set<String> getGroups(String user) throws IOException {
        synchronized (this) {
            rotateIfNeeded();
            if (cachedGroups.containsKey(user)) {
                return cachedGroups.get(user);
            }
        }
        Set<String> groups = getUnixGroups(user);
        if (!groups.isEmpty()) {
            synchronized (this) {
                cachedGroups.put(user, groups);
            }
        }
        return groups;
    }

    private void rotateIfNeeded() {
        long nowMs = Time.currentTimeMillis();
        if (nowMs >= lastRotationMs + timeoutMs) {
            //Rotate once per timeout period that has passed since last time this was called.
            //This is necessary since this method may be called at arbitrary intervals.
            int rotationsToDo = (int) ((nowMs - lastRotationMs) / timeoutMs);
            for (int i = 0; i < rotationsToDo; i++) {
                cachedGroups.rotate();
            }
            lastRotationMs = nowMs;
        }
    }

    /**
     * Get the current user's group list from Unix by running the command 'groups' NOTE. For non-existing user it will return EMPTY list
     *
     * @param user user name
     * @return the groups set that the <code>user</code> belongs to
     *
     * @throws IOException if encounter any error when running the command
     */
    private Set<String> getUnixGroups(final String user) throws IOException {
        if (user == null) {
            LOG.debug("User is null. Returning an empty set as the result");
            return new HashSet<>();
        }

        String result;
        try {
            result = shellCommandRunner.execCommand(ShellUtils.getGroupsForUserCommand(user));
        } catch (ExitCodeException e) {
            // if we didn't get the group - just return empty list;
            LOG.debug("Unable to get groups for user " + user + ". ShellUtils command failed with exit code " + e.getExitCode());
            return new HashSet<>();
        }

        Set<String> groups = new HashSet<>();
        for (String group : result.split(shellCommandRunner.getTokenSeparatorRegex())) {
            groups.add(group);
        }
        return groups;
    }

}
