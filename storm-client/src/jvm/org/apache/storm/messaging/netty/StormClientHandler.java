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

package org.apache.storm.messaging.netty;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.shade.io.netty.channel.ChannelHandlerContext;
import org.apache.storm.shade.io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormClientHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(StormClientHandler.class);
    private final Client client;
    private final KryoValuesDeserializer des;
    private final AtomicBoolean[] remoteBpStatus;

    StormClientHandler(Client client, AtomicBoolean[] remoteBpStatus, Map<String, Object> conf) {
        this.client = client;
        this.remoteBpStatus = remoteBpStatus;
        des = new KryoValuesDeserializer(conf);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
        //examine the response message from server
        if (message instanceof ControlMessage) {
            ControlMessage msg = (ControlMessage) message;
            if (msg == ControlMessage.FAILURE_RESPONSE) {
                LOG.info("failure response:{}", msg);
            }
        } else if (message instanceof BackPressureStatus) {
            BackPressureStatus status = (BackPressureStatus) message;
            if (status.bpTasks != null) {
                for (Integer bpTask : status.bpTasks) {
                    try {
                        remoteBpStatus[bpTask].set(true);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        //Just in case we get something we are confused about
                        // we can continue processing the rest of the tasks
                        LOG.error("BP index out of bounds {}", e);
                    }
                }
            }
            if (status.nonBpTasks != null) {
                for (Integer bpTask : status.nonBpTasks) {
                    try {
                        remoteBpStatus[bpTask].set(false);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        //Just in case we get something we are confused about
                        // we can continue processing the rest of the tasks
                        LOG.error("BP index out of bounds {}", e);
                    }
                }
            }
            LOG.debug("Received BackPressure status update : {}", status);
        } else if (message instanceof List) {
            //This should be the load metrics. 
            //There will usually only be one message, but if there are multiple we only process the latest one.
            List<TaskMessage> list = (List<TaskMessage>) message;
            if (list.size() < 1) {
                throw new RuntimeException("Didn't see enough load metrics (" + client.getDstAddress() + ") " + list);
            }
            TaskMessage tm = list.get(list.size() - 1);
            if (tm.task() != Server.LOAD_METRICS_TASK_ID) {
                throw new RuntimeException("Metrics messages are sent to the system task (" + client.getDstAddress() + ") " + tm);
            }
            List<Object> metrics = des.deserialize(tm.message());
            if (metrics.size() < 1) {
                throw new RuntimeException("No metrics data in the metrics message (" + client.getDstAddress() + ") " + metrics);
            }
            if (!(metrics.get(0) instanceof Map)) {
                throw new RuntimeException("The metrics did not have a map in the first slot (" + client.getDstAddress() + ") " + metrics);
            }
            client.setLoadMetrics((Map<Integer, Double>) metrics.get(0));
        } else {
            throw new RuntimeException("Don't know how to handle a message of type "
                                       + message + " (" + client.getDstAddress() + ")");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!(cause instanceof ConnectException)) {
            if (cause instanceof IOException) {
                LOG.info("Connection to {} failed: {}", client.getDstAddress(), cause.getMessage());
            } else {
                LOG.error("Connection to {} failed: {}", client.getDstAddress(), cause);
            }
        }
    }
}
