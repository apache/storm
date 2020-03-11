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

package org.apache.storm.scheduler.resource.strategies.scheduling;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;

/**
 * An interface to for implementing different scheduling strategies for the resource aware scheduling.
 */
public interface IStrategy {

    enum ObjectResourceSortType { GENERIC, DEFAULT }

    /**
     * Prepare the Strategy for scheduling.
     * TODO: rename this method to initInstance()
     *
     * @param config the cluster configuration
     */
    void prepare(Map<String, Object> config);

    /**
     * Initialize for the default implementation of schedule().
     *
     * @param sortNodesForEachExecutor Sort nodes before scheduling each executor.
     */
    void initForSchedule(boolean sortNodesForEachExecutor, ObjectResourceSortType resSortType);

    /**
     * This method is invoked to calculate a scheduling for topology td.  Cluster will reject any changes that are
     * not for the given topology.  Any changes made to the cluster will be committed if the scheduling is successful.
     * <P></P>
     * NOTE: scheduling occurs as a runnable in an interruptible thread.  Scheduling should consider being interrupted if
     * long running.
     *
     * @param schedulingState the current state of the cluster
     * @param td the topology to schedule for
     * @return returns a SchedulingResult object containing SchedulingStatus object to indicate whether scheduling is
     *     successful.
     */
    SchedulingResult schedule(Cluster schedulingState, TopologyDetails td);

}
