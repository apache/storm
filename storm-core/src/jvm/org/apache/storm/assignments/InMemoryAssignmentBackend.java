/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.assignments;

import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.generated.Assignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An assignment backend which will keep all assignments and id-info in memory. Only used if no backend is specified internal.
 */
public class InMemoryAssignmentBackend implements ILocalAssignmentsBackend {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryAssignmentBackend.class);

    protected Map<String, Assignment> idToAssignment;
    protected Map<String, String> idToName;
    protected Map<String, String> nameToId;
    /**
     * Used for assignments set/get, assignments set/get should be kept thread safe
     */
    private final Object assignmentsLock = new Object();

    public InMemoryAssignmentBackend() {}

    @Override
    public void prepare(Map conf) {
        // do nothing for conf now
        this.idToAssignment = new HashMap<>();
        this.idToName = new HashMap<>();
        this.nameToId = new HashMap<>();
    }

    @Override
    public void keepOrUpdateAssignment(String stormID, Assignment assignment) {
        synchronized (assignmentsLock) {
            this.idToAssignment.put(stormID, assignment);
        }
    }

    @Override
    public Assignment getAssignment(String stormID) {
        synchronized (assignmentsLock) {
            return this.idToAssignment.get(stormID);
        }
    }

    @Override
    public void removeAssignment(String stormID) {
        synchronized (assignmentsLock) {
            this.idToAssignment.remove(stormID);
        }
    }

    @Override
    public List<String> assignments() {
        if(idToAssignment == null) {
            return new ArrayList<>();
        }
        List<String> ret = new ArrayList<>();
        synchronized (assignmentsLock) {
            ret.addAll(this.idToAssignment.keySet());
            return ret;
        }
    }

    @Override
    public Map<String, Assignment> assignmentsInfo() {
        Map<String, Assignment> ret = new HashMap<>();
        synchronized (assignmentsLock) {
            ret.putAll(this.idToAssignment);
        }

        return ret;
    }

    @Override
    public void syncRemoteAssignments(Map<String, byte[]> remote) {
        Map<String, Assignment> tmp = new HashMap<>();
        for(Map.Entry<String, byte[]> entry: remote.entrySet()) {
            tmp.put(entry.getKey(), ClusterUtils.maybeDeserialize(entry.getValue(), Assignment.class));
        }
        this.idToAssignment = tmp;
    }

    @Override
    public void keepStormId(String stormName, String stormID) {
        this.nameToId.put(stormName, stormID);
        this.idToName.put(stormID, stormName);
    }

    @Override
    public String getStormId(String stormName) {
        return this.nameToId.get(stormName);
    }

    @Override
    public void syncRemoteIDS(Map<String, String> remote) {
        Map<String, String> tmpNameToID = new HashMap<>();
        Map<String, String> tmpIDToName = new HashMap<>();
        for(Map.Entry<String, String> entry: remote.entrySet()) {
            tmpIDToName.put(entry.getKey(), entry.getValue());
            tmpNameToID.put(entry.getValue(), entry.getKey());
        }
        this.idToName = tmpIDToName;
        this.nameToId = tmpNameToID;
    }

    @Override
    public void deleteStormId(String stormName) {
        String id = this.nameToId.remove(stormName);
        if (null != id) {
            this.idToName.remove(id);
        }
    }

    @Override
    public void clearStateForStorm(String stormID) {
        synchronized (assignmentsLock) {
            this.idToAssignment.remove(stormID);
        }

        String name = this.idToName.remove(stormID);
        if (null != name) {
            this.nameToId.remove(name);
        }
    }

    @Override
    public void dispose() {
        this.idToAssignment = null;
        this.nameToId = null;
        this.idToName = null;
    }
}
