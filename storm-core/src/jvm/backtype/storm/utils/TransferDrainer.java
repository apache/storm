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
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferDrainer {
    private static final Logger LOG = LoggerFactory.getLogger(TransferDrainer.class);

    public void add(HashMap<String, ArrayList<TaskMessage>> workerTupleSetMap, HashMap<String, IConnection> connections) {
        for (Map.Entry<String, ArrayList<TaskMessage>> entry : workerTupleSetMap.entrySet()) {
            IConnection connection = connections.get(entry.getKey());
            if (connection == null) {
                LOG.warn("Client for remote server: " + entry.getKey() + " not exist," +
                        "we will drop " + entry.getValue().size() + " messages");
                return;
            }

            connection.send(entry.getValue());
        }
    }
}