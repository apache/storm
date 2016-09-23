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
    private Map config;

    public PacemakerClientPool(Map config) {
        this.config = config;
        List<String> serverList = (List<String>)config.get(Config.PACEMAKER_SERVERS);
        if(serverList == null) {
            serverList = new ArrayList<String>();
        }
        else {
            serverList = new ArrayList<String>(serverList);
        }
        Collections.shuffle(serverList);
        if(serverList != null) {
            servers = new ConcurrentLinkedQueue<String>(serverList);
        }
        else {
            servers = new ConcurrentLinkedQueue<String>();
        }
    }
    
    public HBMessage send(HBMessage m) throws PacemakerConnectionException {
        try {
            return getWriteClient().send(m);
        } catch (Exception e) {
            rotateClients();
            throw e;
        }
    }

    public List<HBMessage> sendAll(HBMessage m) throws PacemakerConnectionException {
        List<HBMessage> responses = new ArrayList<HBMessage>();
        LOG.info("Using servers: {}", servers);
        for(String s : servers) {
            try {
                HBMessage response = getClientForServer(s).send(m);
                responses.add(response);
            } catch (PacemakerConnectionException e) {
                //LOG.error("Failed to send message to Pacemaker.");
            }
        }
        if(responses.size() == 0) {
            throw new PacemakerConnectionException("Failed to connect to any Pacemaker.");
        }
        return responses;
    }

    public void close() {
        for(PacemakerClient client : clientForServer.values()) {
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
        if(client == null) {
            client = new PacemakerClient(config, server);
            clientForServer.put(server, client);
        }
        return client;
    }

}
