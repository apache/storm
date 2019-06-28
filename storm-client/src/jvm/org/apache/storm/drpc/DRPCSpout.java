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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
    static final long serialVersionUID = 2387848310969237877L;
    final String function;
    final String localDrpcId;
    SpoutOutputCollector collector;
    List<DRPCInvocationsClient> clients = new ArrayList<>();
    transient LinkedList<Future<Void>> futures = null;
    transient ExecutorService backround = null;

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

    public String getFunction() {
        return function;
    }

    private void reconnectAsync(final DRPCInvocationsClient client) {
        futures.add(backround.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                client.reconnectClient();
                return null;
            }
        }));
    }

    private void reconnectSync(DRPCInvocationsClient client) {
        try {
            LOG.info("reconnecting... ");
            client.reconnectClient(); //Blocking call
        } catch (TException e2) {
            LOG.error("Failed to connect to DRPC server", e2);
        }
    }

    private void checkFutures() {
        Iterator<Future<Void>> i = futures.iterator();
        while (i.hasNext()) {
            Future<Void> f = i.next();
            if (f.isDone()) {
                i.remove();
            }
            try {
                f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        if (localDrpcId == null) {
            backround = new ExtendedThreadPoolExecutor(0, Integer.MAX_VALUE,
                                                        60L, TimeUnit.SECONDS,
                                                        new SynchronousQueue<Runnable>());
            futures = new LinkedList<>();

            int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
            int index = context.getThisTaskIndex();

            int port = ObjectReader.getInt(conf.get(Config.DRPC_INVOCATIONS_PORT));
            List<String> servers = (List<String>) conf.get(Config.DRPC_SERVERS);
            if (servers == null || servers.isEmpty()) {
                throw new RuntimeException("No DRPC servers configured for topology");
            }

            if (numTasks < servers.size()) {
                for (String s : servers) {
                    futures.add(backround.submit(new Adder(s, port, conf)));
                }
            } else {
                int i = index % servers.size();
                futures.add(backround.submit(new Adder(servers.get(i), port, conf)));
            }
        }

    }

    @Override
    public void close() {
        for (DRPCInvocationsClient client : clients) {
            client.close();
        }
    }

    @Override
    public void nextTuple() {
        if (localDrpcId == null) {
            int size = 0;
            synchronized (clients) {
                size = clients.size(); //This will only ever grow, so no need to worry about falling off the end
            }
            for (int i = 0; i < size; i++) {
                DRPCInvocationsClient client;
                synchronized (clients) {
                    client = clients.get(i);
                }
                if (!client.isConnected()) {
                    LOG.warn("DRPCInvocationsClient [{}:{}] is not connected.", client.getHost(), client.getPort());
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
                    reconnectAsync(client);
                    LOG.error("Not authorized to fetch DRPC request from DRPC server", aze);
                } catch (TException e) {
                    reconnectAsync(client);
                    LOG.error("Failed to fetch DRPC request from DRPC server", e);
                } catch (Exception e) {
                    LOG.error("Failed to fetch DRPC request from DRPC server", e);
                }
            }
            checkFutures();
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

        public DRPCMessageId(String id, int index) {
            this.id = id;
            this.index = index;
        }
    }

    private class Adder implements Callable<Void> {
        private String server;
        private int port;
        private Map<String, Object> conf;

        public Adder(String server, int port, Map<String, Object> conf) {
            this.server = server;
            this.port = port;
            this.conf = conf;
        }

        @Override
        public Void call() throws Exception {
            DRPCInvocationsClient c = new DRPCInvocationsClient(conf, server, port);
            synchronized (clients) {
                clients.add(c);
            }
            return null;
        }
    }
}
