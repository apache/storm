/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.generated.NodeInfo;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferDrainer {

    private static final Logger LOG = LoggerFactory.getLogger(TransferDrainer.class);
    private Map<Integer, ArrayList<TaskMessage>> bundles = new HashMap();

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
        HashMap<NodeInfo, ArrayList<ArrayList<TaskMessage>>> bundleMapByDestination = groupBundleByDestination(taskToNode);

        for (Map.Entry<NodeInfo, ArrayList<ArrayList<TaskMessage>>> entry : bundleMapByDestination.entrySet()) {
            NodeInfo node = entry.getKey();
            IConnection conn = connections.get(node);
            if (conn != null) {
                ArrayList<ArrayList<TaskMessage>> bundle = entry.getValue();
                Iterator<TaskMessage> iter = getBundleIterator(bundle);
                if (null != iter && iter.hasNext()) {
                    conn.send(iter);
                }
                entry.getValue().clear();
            } else {
                LOG.warn("Connection not available for hostPort {}", node);
            }
        }
    }

    private HashMap<NodeInfo, ArrayList<ArrayList<TaskMessage>>> groupBundleByDestination(Map<Integer, NodeInfo> taskToNode) {
        HashMap<NodeInfo, ArrayList<ArrayList<TaskMessage>>> result = new HashMap<>();

        for (Entry<Integer, ArrayList<TaskMessage>> entry : bundles.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }
            NodeInfo node = taskToNode.get(entry.getKey());
            if (node != null) {
                ArrayList<ArrayList<TaskMessage>> msgs = result.get(node);
                if (msgs == null) {
                    msgs = new ArrayList<>();
                    result.put(node, msgs);
                }
                msgs.add(entry.getValue());
            } else {
                LOG.warn("No remote destination available for task {}", entry.getKey());
            }
        }
        return result;
    }

    private Iterator<TaskMessage> getBundleIterator(final ArrayList<ArrayList<TaskMessage>> bundle) {

        if (null == bundle) {
            return null;
        }

        return new Iterator<TaskMessage>() {

            private int offset = 0;
            private int size = 0;
            private int bundleOffset = 0;
            private Iterator<TaskMessage> iter = bundle.get(bundleOffset).iterator();

            {
                for (ArrayList<TaskMessage> list : bundle) {
                    size += list.size();
                }
            }

            @Override
            public boolean hasNext() {
                return offset < size;
            }

            @Override
            public TaskMessage next() {
                TaskMessage msg;
                if (iter.hasNext()) {
                    msg = iter.next();
                } else {
                    bundleOffset++;
                    iter = bundle.get(bundleOffset).iterator();
                    msg = iter.next();
                }
                if (null != msg) {
                    offset++;
                }
                return msg;
            }

            @Override
            public void remove() {
                throw new RuntimeException("not supported");
            }
        };
    }


    public void clear() {
        for (ArrayList<TaskMessage> taskMessages : bundles.values()) {
            taskMessages.clear();
        }
    }
}
