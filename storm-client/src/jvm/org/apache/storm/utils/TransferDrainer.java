
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

package org.apache.storm.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferDrainer {

    private static final Logger LOG = LoggerFactory.getLogger(TransferDrainer.class);
    private final Map<Integer, ArrayList<TaskMessage>> bundles = new HashMap<>();

    // Cache the msgs grouped by destination node
    public void add(TaskMessage taskMsg) {
        int destId = taskMsg.task();
        ArrayList<TaskMessage> msgs = bundles.get(destId);
        if (msgs == null) {
            msgs = new ArrayList<>();
            bundles.put(destId, msgs);
        }
        msgs.add(taskMsg);
    }

    public void send(Map<Integer, NodeInfo> taskToNode, Map<NodeInfo, IConnection> connections) {
        HashMap<NodeInfo, Stream<TaskMessage>> bundleMapByDestination = groupBundleByDestination(taskToNode);

        for (Map.Entry<NodeInfo, Stream<TaskMessage>> entry : bundleMapByDestination.entrySet()) {
            NodeInfo node = entry.getKey();
            IConnection conn = connections.get(node);
            if (conn != null) {
                Iterator<TaskMessage> iter = entry.getValue().iterator();
                if (iter.hasNext()) {
                    conn.send(iter);
                }
            } else {
                LOG.warn("Connection not available for hostPort {}", node);
            }
        }
    }

    private HashMap<NodeInfo, Stream<TaskMessage>> groupBundleByDestination(Map<Integer, NodeInfo> taskToNode) {
        HashMap<NodeInfo, Stream<TaskMessage>> result = new HashMap<>();

        for (Entry<Integer, ArrayList<TaskMessage>> entry : bundles.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }
            NodeInfo node = taskToNode.get(entry.getKey());
            if (node != null) {
                result.merge(node, entry.getValue().stream(), Stream::concat);
            } else {
                LOG.warn("No remote destination available for task {}", entry.getKey());
            }
        }
        return result;
    }

    public void clear() {
        for (ArrayList<TaskMessage> taskMessages : bundles.values()) {
            taskMessages.clear();
        }
    }
}
