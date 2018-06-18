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

package org.apache.storm.assignments;

import java.util.List;
import java.util.Map;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.StormBase;

/**
 * An backend storing local resources about assignments distribution, we use this backend to fetch the resource
 * quickly when scheduling topologies.
 *
 * <p>Supported cache:
 * <ul>
 *     <li>id to {@link Assignment} mapping.</li>
 *     <li>id to name mapping.</li>
 *     <li>id to {@link StormBase} mapping.</li>
 * </ul>
 *
 * <p>The main usage for this backend is to reduce access to the StateStore, i.e. ZooKeeper.
 */
public interface ILocalAssignmentsBackend extends AutoCloseable {
    /**
     * Decide if the backend is synchronized from remote state-store.
     */
    boolean isSynchronized();

    /**
     * Mark this backend as synchronized when sync work is done.
     */
    void setSynchronized();

    /**
     * Initial function for creating backend.
     *
     * @param conf config
     */
    void prepare(Map conf);

    /**
     * Keep a storm assignment to local state or update old assignment.
     *
     * @param stormId    storm runtime id
     * @param assignment assignment as thrift
     */
    void keepOrUpdateAssignment(String stormId, Assignment assignment);

    /**
     * Get assignment as {@link Assignment} for a storm.
     *
     * @param stormId storm runtime id
     * @return assignment
     */
    Assignment getAssignment(String stormId);

    /**
     * Remove local assignment cache for a storm.
     * @param stormId storm id to delete for.
     */
    void removeAssignment(String stormId);

    /**
     * List all the storm runtime ids of local assignments.
     *
     * @return a list of storm ids
     */
    List<String> assignments();

    /**
     * Get all the local assignments of local state.
     *
     * @return mapping of storm-id -> assignment
     */
    Map<String, Assignment> assignmentsInfo();

    /**
     * Sync remote assignments to local, only used for leader master instance.
     *
     * @param remote specific remote assignments, if remote is null,
     *               it will sync from remote state store, i.e. ZK.
     */
    void syncRemoteAssignments(Map<String, byte[]> remote);

    /**
     * Get an {@link StormBase} instance for a storm.
     * @param stormId id of storm.
     * @return cached storm base instance.
     */
    StormBase getStormBase(String stormId);

    /**
     * Keep or update an storm base instance.
     */
    void keepOrUpdateStormBase(String id, StormBase base);

    /**
     * Sync remote storm bases to local cache.
     * @param remote specific remote storm bases, if remote is null, it will sync from zookeeper,
     *               this method is only used for leader master instance now.
     */
    void syncRemoteStormBases(Map<String, StormBase> remote);

    /**
     * Remove cached storm base from this backend.
     * @param stormId storm id.
     */
    void removeStormBase(String stormId);

    /**
     * Keep a mapping storm-name -> storm-id to local state.
     *
     * @param stormName storm name
     * @param stormId   storm runtime id
     */
    void keepStormId(String stormName, String stormId);

    /**
     * Get storm runtime id from local.
     *
     * @param stormName name of a storm
     * @return runtime storm id
     */
    String getStormId(String stormName);

    /**
     * Sync remote storm ids to local, will just used for nimbus.
     *
     * @param remote remote ids from state store
     */
    void syncRemoteIds(Map<String, String> remote);

    /**
     * Delete a local cache of stormId which is mapped to a specific storm name.
     *
     * @param stormName storm name
     */
    void deleteStormId(String stormName);

    /**
     * Clear all the state for a storm.
     *
     * @param stormId storm id
     */
    void clearStateForStorm(String stormId);

    /**
     * Function to release resource.
     */
    void close();
}
