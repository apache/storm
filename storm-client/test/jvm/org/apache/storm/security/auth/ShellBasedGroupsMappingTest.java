/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.security.auth;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.utils.ShellCommandRunner;
import org.apache.storm.utils.ShellUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShellBasedGroupsMappingTest {

    private static final String TEST_TWO_GROUPS = "group1 group2";
    private static final String TEST_NO_GROUPS = "";
    private static final String TEST_USER_1 = "TestUserOne";
    private static final String GROUP_SEPARATOR_REGEX = "\\s";
    private static final int CACHE_EXPIRATION_SECS = 10;

    private ShellCommandRunner mockShell;
    private ShellBasedGroupsMapping groupsMapping;
    private Map<String, Object> topoConf;

    @Before
    public void setUp() {
        mockShell = mock(ShellCommandRunner.class);
        groupsMapping = new ShellBasedGroupsMapping(mockShell);
        topoConf = new HashMap<>();
        topoConf.put(Config.STORM_GROUP_MAPPING_SERVICE_CACHE_DURATION_SECS, 10);
        when(mockShell.getTokenSeparatorRegex()).thenReturn(GROUP_SEPARATOR_REGEX);
    }

    @Test
    public void testCanGetGroups() throws Exception {
        try (SimulatedTime t = new SimulatedTime()) {
            groupsMapping.prepare(topoConf);
            when(mockShell.execCommand(ShellUtils.getGroupsForUserCommand(TEST_USER_1))).thenReturn(TEST_TWO_GROUPS);

            Set<String> groups = groupsMapping.getGroups(TEST_USER_1);

            assertThat(groups, containsInAnyOrder(TEST_TWO_GROUPS.split(GROUP_SEPARATOR_REGEX)));
        }
    }

    @Test
    public void testWillCacheGroups() throws Exception {
        try (SimulatedTime t = new SimulatedTime()) {
            groupsMapping.prepare(topoConf);
            when(mockShell.execCommand(ShellUtils.getGroupsForUserCommand(TEST_USER_1))).thenReturn(TEST_TWO_GROUPS, TEST_NO_GROUPS);

            Set<String> firstGroups = groupsMapping.getGroups(TEST_USER_1);
            Set<String> secondGroups = groupsMapping.getGroups(TEST_USER_1);

            assertThat(firstGroups, is(secondGroups));
        }
    }

    @Test
    public void testWillExpireCache() throws Exception {
        try (SimulatedTime t = new SimulatedTime()) {
            groupsMapping.prepare(topoConf);
            when(mockShell.execCommand(ShellUtils.getGroupsForUserCommand(TEST_USER_1))).thenReturn(TEST_TWO_GROUPS, TEST_NO_GROUPS);

            Set<String> firstGroups = groupsMapping.getGroups(TEST_USER_1);
            Time.advanceTimeSecs(CACHE_EXPIRATION_SECS * 2);
            Set<String> secondGroups = groupsMapping.getGroups(TEST_USER_1);

            assertThat(firstGroups, not(secondGroups));
            assertThat(secondGroups, contains(TEST_NO_GROUPS));
        }
    }

}
