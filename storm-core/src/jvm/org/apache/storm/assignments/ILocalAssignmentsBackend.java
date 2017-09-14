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
package org.apache.storm.assignments;

import org.apache.storm.generated.Assignment;

import java.util.List;
import java.util.Map;

/**
 * Interface for storing local assignments.
 */
public interface ILocalAssignmentsBackend {

    /**
     * initial function for creating backend.
     * @param conf
     */
    void prepare(Map conf);

    /**
     * Keep a storm assignment to local state or update old assignment.
     * @param stormID storm runtime id
     * @param assignment assignment as thrift
     */
    void keepOrUpdateAssignment(String stormID, Assignment assignment);

    /**
     * Get assignment as byte[] for a storm
     * @param stormID storm runtime id
     * @return
     */
    Assignment getAssignment(String stormID);

    void removeAssignment(String stormID);

    /**
     * List all the storm runtime ids of local assignments
     * @return a list of storm ids
     */
    List<String> assignments();

    /**
     * Get all the local assignments of local state
     * @return mapping of storm-id -> assignment
     */
    Map<String, Assignment> assignmentsInfo();

    /**
     * Sync remote assignments to local, if remote is null, we will sync it from zk
     * @param remote specific remote assignments, if is null, it will sync from zookeeper[only used for nimbus]
     */
    void syncRemoteAssignments(Map<String, byte[]> remote);

    /**
     * Keep a mapping storm-name -> storm-id to local state
     * @param stormName
     * @param stormID storm runtime id
     */
    void keepStormId(String stormName, String stormID);

    /**
     * Get storm runtime id from local
     * @param stormName name of a storm
     * @return
     */
    String getStormId(String stormName);

    /**
     * sync remote storm ids to local, will just used for nimbus.
     * @param remote
     */
    void syncRemoteIDS(Map<String, String> remote);

    void deleteStormId(String stormName);

    /**
     * clear all the state for a storm
     * @param stormID
     */
    void clearStateForStorm(String stormID);

    /**
     * function to release resource
     */
    void dispose();

}
