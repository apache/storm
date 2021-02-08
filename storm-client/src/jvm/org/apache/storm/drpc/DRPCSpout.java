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

package org.apache.storm.drpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.ILocalDRPC;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.generated.DistributedRPCInvocations;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.ExtendedThreadPoolExecutor;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class DRPCSpout extends BaseRichSpout {
    public static final Logger LOG = LoggerFactory.getLogger(DRPCSpout.class);
    //ANY CHANGE TO THIS CODE MUST BE SERIALIZABLE COMPATIBLE OR THERE WILL BE PROBLEMS
    private static final long serialVersionUID = 2387848310969237877L;
    private static final int clientConstructionRetryIntervalSec = 120;
    private final String function;
    private final String localDrpcId;
    private SpoutOutputCollector collector;
    private List<DRPCInvocationsClient> clients = Collections.synchronizedList(new ArrayList<>());
    private transient ExecutorService background = null;
    private transient Map<String, CompletableFuture<Void>> futuresMap = null;  // server : future

    public DRPCSpout(String function) {
        this.function = function;
        if (DRPCClient.isLocalOverride()) {
            localDrpcId = DRPCClient.getOverrideServiceId();
        } else {
            localDrpcId = null;
        }
    }


    public DRPCSpout(String function, ILocalDRPC drpc) {
        this.function = function;
        localDrpcId = drpc.getServiceId();
    }

    public String get_function() {
        return function;
    }

    private void reconnectAsync(final DRPCInvocationsClient client) {
        String remote = client.getHost();
        CompletableFuture<Void> future = futuresMap.get(remote);
        if (future.isDone()) {
            LOG.warn("DRPCInvocationsClient [{}:{}] connection failed, no pending reconnection. Try reconnecting...",
                client.getHost(), client.getPort());
            CompletableFuture<Void> newFuture =
                CompletableFuture.runAsync(() -> {
                    try {
                        client.reconnectClient();
                        LOG.info("Reconnected to remote {}:{}. ",
                            client.getHost(), client.getPort());
                    } catch (Exception e) {
                        collector.reportError(e);
                        LOG.warn("Failed to reconnect to remote {}:{}. ",
                            client.getHost(), client.getPort(), e);
                    }
                }, background);
            futuresMap.put(remote, newFuture);
        }
    }

    private void reconnectSync(DRPCInvocationsClient client) {
        try {
            LOG.info("reconnecting... ");
            client.reconnectClient(); //Blocking call
        } catch (TException e2) {
            LOG.error("Failed to connect to DRPC server", e2);
        }
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        if (localDrpcId == null) {
            background = new ExtendedThreadPoolExecutor(0, Integer.MAX_VALUE,
                                                        60L, TimeUnit.SECONDS,
                                                        new SynchronousQueue<Runnable>());
            futuresMap = new HashMap<>();
            int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
            int index = context.getThisTaskIndex();

            int port = ObjectReader.getInt(conf.get(Config.DRPC_INVOCATIONS_PORT));
            List<String> servers = (List<String>) conf.get(Config.DRPC_SERVERS);
            if (servers == null || servers.isEmpty()) {
                throw new RuntimeException("No DRPC servers configured for topology");
            }

            List<DRPCClientBuilder> clientBuilders = new ArrayList<>();
            if (numTasks < servers.size()) {
                for (String s : servers) {
                    clientBuilders.add(new DRPCClientBuilder(s, port, conf));
                }
            } else {
                int i = index % servers.size();
                clientBuilders.add(new DRPCClientBuilder(servers.get(i), port, conf));
            }
            establishConnections(clientBuilders);
        }
    }

    protected void establishConnections(List<DRPCClientBuilder> clientBuilders) {
        int numOfClients = clientBuilders.size();

        for (int i = 0; i < numOfClients; i++) {
            DRPCClientBuilder builder = clientBuilders.get(i);
            String server = builder.getServer();
            CompletableFuture<Void> future = CompletableFuture.runAsync(builder, background);
            futuresMap.put(server, future);
        }
    }

    @Override
    public void close() {
        for (DRPCInvocationsClient client : clients) {
            client.close();
        }
        if (background != null) {
            background.shutdownNow();
        }
    }

    @Override
    public void nextTuple() {
        if (localDrpcId == null) {
            //This will only ever grow and at least one client has been up
            for (int i = 0; i < clients.size(); i++) {
                DRPCInvocationsClient client = clients.get(i);
                if (!client.isConnected()) {
                    reconnectAsync(client);
                    continue;
                }
                try {
                    DRPCRequest req = client.fetchRequest(function);
                    if (req.get_request_id().length() > 0) {
                        Map<String, Object> returnInfo = new HashMap<>();
                        returnInfo.put("id", req.get_request_id());
                        returnInfo.put("host", client.getHost());
                        returnInfo.put("port", client.getPort());
                        collector.emit(new Values(req.get_func_args(), JSONValue.toJSONString(returnInfo)),
                                        new DRPCMessageId(req.get_request_id(), i));
                        break;
                    }
                } catch (AuthorizationException aze) {
                    LOG.error("Not authorized to fetch DRPC request from DRPC server [{}:{}]",
                        client.getHost(), client.getPort(), aze);
                    reconnectAsync(client);
                } catch (Exception e) {
                    LOG.error("Failed to fetch DRPC request from DRPC server [{}:{}]",
                        client.getHost(), client.getPort(), e);
                    reconnectAsync(client);
                }
            }
        } else {
            DistributedRPCInvocations.Iface drpc = (DistributedRPCInvocations.Iface) ServiceRegistry.getService(localDrpcId);
            if (drpc != null) { // can happen during shutdown of drpc while topology is still up
                try {
                    DRPCRequest req = drpc.fetchRequest(function);
                    if (req.get_request_id().length() > 0) {
                        Map<String, Object> returnInfo = new HashMap<>();
                        returnInfo.put("id", req.get_request_id());
                        returnInfo.put("host", localDrpcId);
                        returnInfo.put("port", 0);
                        collector.emit(new Values(req.get_func_args(), JSONValue.toJSONString(returnInfo)),
                                        new DRPCMessageId(req.get_request_id(), 0));
                    }
                } catch (AuthorizationException aze) {
                    throw new RuntimeException(aze);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
        DRPCMessageId did = (DRPCMessageId) msgId;
        DistributedRPCInvocations.Iface client;

        if (localDrpcId == null) {
            client = clients.get(did.index);
        } else {
            client = (DistributedRPCInvocations.Iface) ServiceRegistry.getService(localDrpcId);
        }

        int retryCnt = 0;
        int maxRetries = 3;

        while (retryCnt < maxRetries) {
            retryCnt++;
            try {
                client.failRequest(did.id);
                break;
            } catch (AuthorizationException aze) {
                LOG.error("Not authorized to failRequest from DRPC server", aze);
                throw new RuntimeException(aze);
            } catch (TException tex) {
                if (retryCnt >= maxRetries) {
                    LOG.error("Failed to fail request", tex);
                    break;
                }
                reconnectSync((DRPCInvocationsClient) client);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("args", "return-info"));
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    private static class DRPCMessageId {
        String id;
        int index;

        DRPCMessageId(String id, int index) {
            this.id = id;
            this.index = index;
        }
    }

    private class DRPCClientBuilder implements Runnable {
        private String server;
        private int port;
        private Map<String, Object> conf;

        DRPCClientBuilder(String server, int port, Map<String, Object> conf) {
            this.server = server;
            this.port = port;
            this.conf = conf;
        }

        @Override
        public void run() {
            DRPCInvocationsClient c = null;
            while (c == null) {
                try {
                    // DRPCInvocationsClient has backoff retry logic
                    c = new DRPCInvocationsClient(conf, server, port);
                } catch (Exception e) {
                    collector.reportError(e);
                    LOG.error("Failed to create DRPCInvocationsClient for remote {}:{}. Retrying after {} secs.",
                        server, port, clientConstructionRetryIntervalSec, e);
                    try {
                        Thread.sleep(clientConstructionRetryIntervalSec * 1000);
                    } catch (InterruptedException ex) {
                        LOG.warn("DRPCInvocationsClient creation retry sleep interrupted.");
                        break;
                    }
                }
            }
            if (c != null) {
                LOG.info("Successfully created DRPCInvocationsClient for remote {}:{}.", server, port);
                clients.add(c);
            } else {
                LOG.warn("DRPCInvocationsClient creation retry for remote {}:{} interrupted.",
                    server, port);
            }
        }

        public String getServer() {
            return server;
        }
    }
}
