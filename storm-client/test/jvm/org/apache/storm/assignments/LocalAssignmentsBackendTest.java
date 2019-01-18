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

package org.apache.storm.assignments;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.shade.org.apache.commons.collections.map.HashedMap;
import org.apache.storm.utils.ConfigUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LocalAssignmentsBackendTest {

    @Test
    public void testLocalAssignment() {
        Map<String, Assignment> stormToAssignment = new HashMap<>();
        String storm1 = "storm1";
        String storm2 = "storm2";
        Assignment ass1 = mockedAssignment(1);
        Assignment ass2 = mockedAssignment(2);

        ILocalAssignmentsBackend backend = LocalAssignmentsBackendFactory.getBackend(ConfigUtils.readStormConfig());
        assertEquals(null, backend.getAssignment(storm1));
        backend.keepOrUpdateAssignment(storm1, ass1);
        backend.keepOrUpdateAssignment(storm2, ass2);
        assertEquals(ass1, backend.getAssignment(storm1));
        assertEquals(ass2, backend.getAssignment(storm2));
        backend.clearStateForStorm(storm1);
        assertEquals(null, backend.getAssignment(storm1));
        backend.keepOrUpdateAssignment(storm1, ass1);
        backend.keepOrUpdateAssignment(storm1, ass2);
        assertEquals(ass2, backend.getAssignment(storm1));
    }

    @Test
    public void testLocalIdInfo() {
        String name1 = "name1";
        String name2 = "name2";
        String name3 = "name3";

        String id1 = "id1";
        String id2 = "id2";
        String id3 = "id3";

        ILocalAssignmentsBackend backend = LocalAssignmentsBackendFactory.getBackend(ConfigUtils.readStormConfig());
        assertEquals(null, backend.getStormId(name3));
        backend.keepStormId(name1, id1);
        backend.keepStormId(name2, id2);
        assertEquals(id1, backend.getStormId(name1));
        assertEquals(id2, backend.getStormId(name2));
        backend.deleteStormId(name1);
        assertEquals(null, backend.getStormId(name1));
        backend.clearStateForStorm(id2);
        assertEquals(null, backend.getStormId(name2));
        backend.keepStormId(name1, id1);
        backend.keepStormId(name1, id3);
        assertEquals(id3, backend.getStormId(name1));
    }

    private Assignment mockedAssignment(int i) {
        Assignment ass = new Assignment();
        ass.set_master_code_dir("master_code_dir" + i);
        HashMap node_to_host = new HashMap();
        node_to_host.put("node" + i, "host" + i);
        ass.set_node_host(node_to_host);
        Map<List<Long>, NodeInfo> executor_node_port = new HashMap<>();
        Set<Long> nodePorts = new HashSet<>();
        nodePorts.add(9723L);
        executor_node_port.put(Arrays.asList(i + 0L), new NodeInfo("node" + i, nodePorts));
        ass.set_executor_node_port(executor_node_port);
        Map<List<Long>, Long> executor_start_time_secs = new HashMap<>();
        executor_start_time_secs.put(Arrays.asList(1L), 12345L);
        ass.set_executor_start_time_secs(executor_start_time_secs);
        ass.set_worker_resources(new HashedMap());
        ass.set_total_shared_off_heap(new HashedMap());
        ass.set_owner("o");
        return ass;
    }
}
