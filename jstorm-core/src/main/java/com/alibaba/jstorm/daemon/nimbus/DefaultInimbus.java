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
package com.alibaba.jstorm.daemon.nimbus;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.WorkerSlot;

public class DefaultInimbus implements INimbus {

    @Override
    public void prepare(Map stormConf, String schedulerLocalDir) {
        // TODO Auto-generated method stub

    }

    @Override
    public Collection<WorkerSlot> allSlotsAvailableForScheduling(
            Collection<SupervisorDetails> existingSupervisors,
            Topologies topologies, Set<String> topologiesMissingAssignments) {
        // TODO Auto-generated method stub
        Collection<WorkerSlot> result = new HashSet<WorkerSlot>();
        for (SupervisorDetails detail : existingSupervisors) {
            for (Integer port : detail.getAllPorts())
                result.add(new WorkerSlot(detail.getId(), port));
        }
        return result;
    }

    @Override
    public void assignSlots(Topologies topologies,
            Map<String, Collection<WorkerSlot>> newSlotsByTopologyId) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getHostName(
            Map<String, SupervisorDetails> existingSupervisors, String nodeId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IScheduler getForcedScheduler() {
        // TODO Auto-generated method stub
        return null;
    }

}
