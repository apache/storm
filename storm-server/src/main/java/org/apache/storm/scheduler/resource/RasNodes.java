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

package org.apache.storm.scheduler.resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RasNodes {

    private static final Logger LOG = LoggerFactory.getLogger(RasNodes.class);
    private final Map<String, RasNode> nodeMap;

    public RasNodes(Cluster cluster) {
        this.nodeMap = getAllNodesFrom(cluster);
    }

    public static Map<String, RasNode> getAllNodesFrom(Cluster cluster) {

        //A map of node ids to node objects
        Map<String, RasNode> nodeIdToNode = new HashMap<>();
        //A map of assignments organized by node with the following format:
        //{nodeId -> {topologyId -> {workerId -> {execs}}}}
        Map<String, Map<String, Map<String, Collection<ExecutorDetails>>>> assignmentRelationshipMap = new HashMap<>();

        Map<String, Map<String, WorkerSlot>> workerIdToWorker = new HashMap<>();
        for (SchedulerAssignment assignment : cluster.getAssignments().values()) {
            String topId = assignment.getTopologyId();

            for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry :
                assignment.getSlotToExecutors().entrySet()) {
                WorkerSlot slot = entry.getKey();
                String nodeId = slot.getNodeId();
                if (!assignmentRelationshipMap.containsKey(nodeId)) {
                    assignmentRelationshipMap.put(
                        nodeId, new HashMap<>());
                    workerIdToWorker.put(nodeId, new HashMap<>());
                }
                workerIdToWorker.get(nodeId).put(slot.getId(), slot);
                if (!assignmentRelationshipMap.get(nodeId).containsKey(topId)) {
                    assignmentRelationshipMap
                        .get(nodeId)
                        .put(topId, new HashMap<>());
                }
                if (!assignmentRelationshipMap.get(nodeId).get(topId).containsKey(slot.getId())) {
                    assignmentRelationshipMap
                        .get(nodeId)
                        .get(topId)
                        .put(slot.getId(), new LinkedList<>());
                }
                Collection<ExecutorDetails> execs = entry.getValue();
                assignmentRelationshipMap.get(nodeId).get(topId).get(slot.getId()).addAll(execs);
            }
        }

        for (SupervisorDetails sup : cluster.getSupervisors().values()) {
            //Initialize a worker slot for every port even if there is no assignment to it
            for (int port : sup.getAllPorts()) {
                WorkerSlot worker = new WorkerSlot(sup.getId(), port);
                if (!workerIdToWorker.containsKey(sup.getId())) {
                    workerIdToWorker.put(sup.getId(), new HashMap<>());
                }
                if (!workerIdToWorker.get(sup.getId()).containsKey(worker.getId())) {
                    workerIdToWorker.get(sup.getId()).put(worker.getId(), worker);
                }
            }
            nodeIdToNode.put(
                sup.getId(),
                new RasNode(
                    sup.getId(),
                    sup,
                    cluster,
                    workerIdToWorker.get(sup.getId()),
                    assignmentRelationshipMap.get(sup.getId())));
        }

        //Add in supervisors that might have crashed but workers are still alive
        for (Map.Entry<String, Map<String, Map<String, Collection<ExecutorDetails>>>> entry :
            assignmentRelationshipMap.entrySet()) {
            String nodeId = entry.getKey();
            Map<String, Map<String, Collection<ExecutorDetails>>> assignments = entry.getValue();
            if (!nodeIdToNode.containsKey(nodeId)) {
                LOG.info(
                    "Found an assigned slot(s) on a dead supervisor {} with assignments {}",
                    nodeId,
                    assignments);
                nodeIdToNode.put(
                    nodeId, new RasNode(nodeId, null, cluster, workerIdToWorker.get(nodeId), assignments));
            }
        }
        return nodeIdToNode;
    }

    /**
     * get node object from nodeId.
     */
    public RasNode getNodeById(String nodeId) {
        return this.nodeMap.get(nodeId);
    }

    /**
     * Free everything on the given slots.
     *
     * @param workerSlots the slots to free
     */
    public void freeSlots(Collection<WorkerSlot> workerSlots) {
        for (RasNode node : nodeMap.values()) {
            for (WorkerSlot ws : node.getUsedSlots()) {
                if (workerSlots.contains(ws)) {
                    LOG.debug("freeing ws {} on node {}", ws, node);
                    node.free(ws);
                }
            }
        }
    }

    public Collection<RasNode> getNodes() {
        return this.nodeMap.values();
    }

    /**
     * Get a map with list of RasNode for each hostname.
     *
     * @return map of hostname to a list of RasNode
     */
    public Map<String, List<RasNode>> getHostnameToNodes() {
        Map<String, List<RasNode>> hostnameToNodes = new HashMap<>();
        nodeMap.values()
                .forEach(node -> hostnameToNodes.computeIfAbsent(node.getHostname(), (hn) -> new ArrayList<>()).add(node));
        return hostnameToNodes;
    }

    /**
     * Get a map from RasNodeId to HostName.
     *
     * @return map of nodeId to hostname
     */
    public Map<String, String> getNodeIdToHostname() {
        Map<String, String> nodeIdToHostname = new HashMap<>();
        nodeMap.values()
                .forEach(node -> nodeIdToHostname.put(node.getId(), node.getHostname()));
        return nodeIdToHostname;
    }

    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder();
        for (RasNode node : nodeMap.values()) {
            ret.append(node).append("\n");
        }
        return ret.toString();
    }
}
