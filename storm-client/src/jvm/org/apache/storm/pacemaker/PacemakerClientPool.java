/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.pacemaker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.storm.Config;
import org.apache.storm.generated.HBMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PacemakerClientPool {

    private static final Logger LOG = LoggerFactory.getLogger(PacemakerClientPool.class);

    private ConcurrentHashMap<String, PacemakerClient> clientForServer = new ConcurrentHashMap<>();
    private ConcurrentLinkedQueue<String> servers;
    private Map<String, Object> config;

    public PacemakerClientPool(Map<String, Object> config) {
        this.config = config;
        List<String> serverList = (List<String>) config.get(Config.PACEMAKER_SERVERS);
        if (serverList == null) {
            serverList = new ArrayList<>();
        } else {
            serverList = new ArrayList<>(serverList);
        }
        Collections.shuffle(serverList);
        if (serverList != null) {
            servers = new ConcurrentLinkedQueue<>(serverList);
        } else {
            servers = new ConcurrentLinkedQueue<>();
        }
    }

    public HBMessage send(HBMessage m) throws PacemakerConnectionException, InterruptedException {
        try {
            return getWriteClient().send(m);
        } catch (PacemakerConnectionException e) {
            rotateClients();
            throw e;
        }
    }

    public List<HBMessage> sendAll(HBMessage m) throws PacemakerConnectionException, InterruptedException {
        List<HBMessage> responses = new ArrayList<HBMessage>();
        LOG.debug("Using servers: {}", servers);
        for (String s : servers) {
            try {
                HBMessage response = getClientForServer(s).send(m);
                responses.add(response);
            } catch (PacemakerConnectionException e) {
                LOG.warn("Failed to connect to the pacemaker server {}, attempting to reconnect", s);
                getClientForServer(s).reconnect();
            }
        }
        if (responses.size() == 0) {
            throw new PacemakerConnectionException("Failed to connect to any Pacemaker.");
        }
        return responses;
    }

    public void close() {
        for (PacemakerClient client : clientForServer.values()) {
            client.shutdown();
            client.close();
        }
    }

    private void rotateClients() {
        PacemakerClient c = getWriteClient();
        String server = servers.peek();
        // Servers should be rotated **BEFORE** the old client is removed from clientForServer
        // or a race with getWriteClient() could cause it to be put back in the map.
        servers.add(servers.remove());
        clientForServer.remove(server);
        c.shutdown();
        c.close();
    }

    private PacemakerClient getWriteClient() {
        return getClientForServer(servers.peek());
    }

    private PacemakerClient getClientForServer(String server) {
        PacemakerClient client = clientForServer.get(server);
        if (client == null) {
            client = new PacemakerClient(config, server);
            clientForServer.put(server, client);
        }
        return client;
    }
}
