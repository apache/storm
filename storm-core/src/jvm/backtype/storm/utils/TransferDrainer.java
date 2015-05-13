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
package backtype.storm.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

public class TransferDrainer {

  private ArrayList<TaskMessage> buffer = new ArrayList<TaskMessage>();

  public void addAll(ArrayList<TaskMessage> tuples) {
    buffer.addAll(tuples);
  }
  
  public void send(HashMap<Integer, String> taskToHostPort, HashMap<String, IConnection> connections) {
    HashMap<String, ArrayList<TaskMessage>> messageGroupedByDest = groupMessageByDestination(taskToHostPort);
    for (String hostPort : messageGroupedByDest.keySet()) {
      IConnection connection = connections.get(hostPort);
      if (null != connection) { 
        ArrayList<TaskMessage> bundle = messageGroupedByDest.get(hostPort);
        Iterator<TaskMessage> iter = bundle.iterator();
        if (null != iter && iter.hasNext()) {
          connection.send(iter);
        }
      }
    } 
  }

  private HashMap<String, ArrayList<TaskMessage>> groupMessageByDestination(HashMap<Integer, String> taskToHostPort) {
    HashMap<String, ArrayList<TaskMessage>> groupedMessage = new HashMap<String, ArrayList<TaskMessage>>();
    for (TaskMessage message : buffer) {
      int taskId = message.task();

      String hostAndPort = taskToHostPort.get(taskId);
      if (null != hostAndPort) {
        ArrayList<TaskMessage> messages = groupedMessage.get(hostAndPort);

        if (null == messages) {
          messages = new ArrayList<TaskMessage>();
          groupedMessage.put(hostAndPort, messages);
        }

        messages.add(message);
      }
    }
    return groupedMessage;
  }

  public void clear() {
    buffer.clear();
  }
}