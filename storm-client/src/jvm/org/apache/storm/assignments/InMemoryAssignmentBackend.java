/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.assignments;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.generated.Assignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An assignment backend which will keep all assignments and id-info in memory. Only used if no backend is specified internal.
 *
 * <p>About thread safe: idToAssignment,idToName,nameToId are all memory cache in nimbus local, for
 * <ul>
 * <li>idToAssignment: nimbus will modify it and supervisors will sync it at fixed interval,
 * so the assignments would come to eventual consistency.</li>
 * <li>idToName: storm submitting/killing is guarded by the same lock, a {@link ConcurrentHashMap} is ok.</li>
 * <li>nameToId: same as <i>idToName</i>.
 * </ul>
 */
public class InMemoryAssignmentBackend implements ILocalAssignmentsBackend {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryAssignmentBackend.class);

    private Map<String, Assignment> idToAssignment;
    private Map<String, String> idToName;
    private Map<String, String> nameToId;
    private volatile boolean isSynchronized = false;

    @Override
    public boolean isSynchronized() {
        return this.isSynchronized;
    }

    @Override
    public void setSynchronized() {
        this.isSynchronized = true;
    }

    @Override
    public void prepare(Map conf) {
        // do nothing for conf now
        this.idToAssignment = new ConcurrentHashMap<>();
        this.idToName = new ConcurrentHashMap<>();
        this.nameToId = new ConcurrentHashMap<>();
    }

    @Override
    public void keepOrUpdateAssignment(String stormId, Assignment assignment) {
        this.idToAssignment.put(stormId, assignment);
    }

    @Override
    public Assignment getAssignment(String stormId) {
        return this.idToAssignment.get(stormId);
    }

    @Override
    public void removeAssignment(String stormId) {
        this.idToAssignment.remove(stormId);
    }

    @Override
    public List<String> assignments() {
        if (idToAssignment == null) {
            return new ArrayList<>();
        }
        List<String> ret = new ArrayList<>();
        ret.addAll(this.idToAssignment.keySet());
        return ret;
    }

    @Override
    public Map<String, Assignment> assignmentsInfo() {
        Map<String, Assignment> ret = new HashMap<>();
        ret.putAll(this.idToAssignment);

        return ret;
    }

    @Override
    public void syncRemoteAssignments(Map<String, byte[]> remote) {
        Map<String, Assignment> tmp = new ConcurrentHashMap<>();
        for (Map.Entry<String, byte[]> entry : remote.entrySet()) {
            tmp.put(entry.getKey(), ClusterUtils.maybeDeserialize(entry.getValue(), Assignment.class));
        }
        this.idToAssignment = tmp;
    }

    @Override
    public void keepStormId(String stormName, String stormId) {
        this.nameToId.put(stormName, stormId);
        this.idToName.put(stormId, stormName);
    }

    @Override
    public String getStormId(String stormName) {
        return this.nameToId.get(stormName);
    }

    @Override
    public void syncRemoteIds(Map<String, String> remote) {
        Map<String, String> tmpNameToId = new ConcurrentHashMap<>();
        Map<String, String> tmpIdToName = new ConcurrentHashMap<>();
        for (Map.Entry<String, String> entry : remote.entrySet()) {
            tmpIdToName.put(entry.getKey(), entry.getValue());
            tmpNameToId.put(entry.getValue(), entry.getKey());
        }
        this.idToName = tmpIdToName;
        this.nameToId = tmpNameToId;
    }

    @Override
    public void deleteStormId(String stormName) {
        String id = this.nameToId.remove(stormName);
        if (null != id) {
            this.idToName.remove(id);
        }
    }

    @Override
    public void clearStateForStorm(String stormId) {
        this.idToAssignment.remove(stormId);

        String name = this.idToName.remove(stormId);
        if (null != name) {
            this.nameToId.remove(name);
        }
    }

    @Override
    public void close() {
        this.idToAssignment = null;
        this.nameToId = null;
        this.idToName = null;
    }
}
