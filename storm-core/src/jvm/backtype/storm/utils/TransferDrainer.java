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

import java.util.*;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

public class TransferDrainer {
  public void send(HashMap<Integer, String> taskToHostPort, HashMap<String, IConnection> connections,
                   List<TaskMessage> buffer) {
    Map<String, List<TaskMessage>> messageGroupedByDest = groupMessageByDestination(taskToHostPort, buffer);
    for (String hostPort : messageGroupedByDest.keySet()) {
      IConnection connection = connections.get(hostPort);
      if (null != connection) { 
        List<TaskMessage> bundle = messageGroupedByDest.get(hostPort);
        Iterator<TaskMessage> iter = bundle.iterator();
        if (null != iter && iter.hasNext()) {
          connection.send(iter);
        }
      }
    } 
  }

  private HashMap<String, List<TaskMessage>> groupMessageByDestination(HashMap<Integer, String> taskToHostPort,
                                                                            List<TaskMessage> buffer) {
    HashMap<String, List<TaskMessage>> groupedMessage = new HashMap<String, List<TaskMessage>>();
    for (TaskMessage message : buffer) {
      int taskId = message.task();

      String hostAndPort = taskToHostPort.get(taskId);
      if (null != hostAndPort) {
        List<TaskMessage> messages = groupedMessage.get(hostAndPort);

        if (null == messages) {
          messages = new ArrayList<TaskMessage>();
          groupedMessage.put(hostAndPort, messages);
        }

        messages.add(message);
      }
    }
    return groupedMessage;
  }
}