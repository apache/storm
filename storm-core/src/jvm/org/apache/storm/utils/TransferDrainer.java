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
package org.apache.storm.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.storm.generated.NodeInfo;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.TaskMessage;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferDrainer {

  private Map<Integer, ArrayList<ArrayList<TaskMessage>>> bundles = new HashMap();
  private static final Logger LOG = LoggerFactory.getLogger(TransferDrainer.class);
  
  public void add(HashMap<Integer, ArrayList<TaskMessage>> taskTupleSetMap) {
    for (Map.Entry<Integer, ArrayList<TaskMessage>> entry : taskTupleSetMap.entrySet()) {
      addListRefToMap(this.bundles, entry.getKey(), entry.getValue());
    }
  }
  
  public void send(Map<Integer, NodeInfo> taskToNode, Map<NodeInfo, IConnection> connections) {
    HashMap<NodeInfo, ArrayList<ArrayList<TaskMessage>>> bundleMapByDestination = groupBundleByDestination(taskToNode);

    for (Map.Entry<NodeInfo, ArrayList<ArrayList<TaskMessage>>> entry : bundleMapByDestination.entrySet()) {
      NodeInfo hostPort = entry.getKey();
      IConnection connection = connections.get(hostPort);
      if (null != connection) {
        ArrayList<ArrayList<TaskMessage>> bundle = entry.getValue();
        Iterator<TaskMessage> iter = getBundleIterator(bundle);
        if (null != iter && iter.hasNext()) {
          connection.send(iter);
        }
      } else {
        LOG.warn("Connection is not available for hostPort {}", hostPort);
      }
    }
  }

  private HashMap<NodeInfo, ArrayList<ArrayList<TaskMessage>>> groupBundleByDestination(Map<Integer, NodeInfo> taskToNode) {
    HashMap<NodeInfo, ArrayList<ArrayList<TaskMessage>>> bundleMap = Maps.newHashMap();
    for (Integer task : this.bundles.keySet()) {
      NodeInfo hostPort = taskToNode.get(task);
      if (hostPort != null) {
        for (ArrayList<TaskMessage> chunk : this.bundles.get(task)) {
          addListRefToMap(bundleMap, hostPort, chunk);
        }
      } else {
        LOG.warn("No remote destination available for task {}", task);
      }
    }
    return bundleMap;
  }

  private <T> void addListRefToMap(Map<T, ArrayList<ArrayList<TaskMessage>>> bundleMap,
                                   T key, ArrayList<TaskMessage> tuples) {
    ArrayList<ArrayList<TaskMessage>> bundle = bundleMap.get(key);

    if (null == bundle) {
      bundle = new ArrayList<ArrayList<TaskMessage>>();
      bundleMap.put(key, bundle);
    }

    if (null != tuples && tuples.size() > 0) {
      bundle.add(tuples);
    }
  }

  private Iterator<TaskMessage> getBundleIterator(final ArrayList<ArrayList<TaskMessage>> bundle) {
    
    if (null == bundle) {
      return null;
    }
    
    return new Iterator<TaskMessage> () {
      
      private int offset = 0;
      private int size = 0;
      {
        for (ArrayList<TaskMessage> list : bundle) {
            size += list.size();
        }
      }
      
      private int bundleOffset = 0;
      private Iterator<TaskMessage> iter = bundle.get(bundleOffset).iterator();
      
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
    bundles.clear();
  }
}
