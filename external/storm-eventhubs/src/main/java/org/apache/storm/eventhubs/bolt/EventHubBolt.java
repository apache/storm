/*******************************************************************************
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
 *******************************************************************************/

package org.apache.storm.eventhubs.bolt;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionSender;
import com.microsoft.azure.servicebus.ServiceBusException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.storm.eventhubs.spout.EventHubException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bolt that writes event message to EventHub.
 */
public class EventHubBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory
        .getLogger(EventHubBolt.class);

    protected OutputCollector collector;
    protected PartitionSender sender;
    protected EventHubClient ehClient;
    protected EventHubBoltConfig boltConfig;

    public EventHubBolt(String connectionString, String entityPath) {
        boltConfig = new EventHubBoltConfig(connectionString, entityPath);
    }

    public EventHubBolt(String userName, String password, String namespace,
                        String entityPath, boolean partitionMode) {
        boltConfig = new EventHubBoltConfig(userName, password, namespace,
                                            entityPath, partitionMode);
    }

    public EventHubBolt(EventHubBoltConfig config) {
        boltConfig = config;
    }

    @Override
    public void prepare(Map<String, Object> config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        String myPartitionId = null;
        if (boltConfig.getPartitionMode()) {
            // We can use the task index (starting from 0) as the partition ID
            myPartitionId = "" + context.getThisTaskIndex();
        }
        logger.info("creating sender: " + boltConfig.getConnectionString()
                    + ", " + boltConfig.getEntityPath() + ", " + myPartitionId);
        try {
            ehClient = EventHubClient.createFromConnectionStringSync(boltConfig.getConnectionString());
            if (boltConfig.getPartitionMode()) {
                sender = ehClient.createPartitionSenderSync(Integer.toString(context.getThisTaskIndex()));
            }
        } catch (Exception ex) {
            collector.reportError(ex);
            throw new RuntimeException(ex);
        }

    }

    @Override
    public void execute(Tuple tuple) {
        try {
            EventData sendEvent = new EventData(boltConfig.getEventDataFormat().serialize(tuple));
            if (boltConfig.getPartitionMode() && sender != null) {
                sender.sendSync(sendEvent);
            } else if (boltConfig.getPartitionMode() && sender == null) {
                throw new EventHubException("Sender is null");
            } else if (!boltConfig.getPartitionMode() && ehClient != null) {
                ehClient.sendSync(sendEvent);
            } else if (!boltConfig.getPartitionMode() && ehClient == null) {
                throw new EventHubException("ehclient is null");
            }
            collector.ack(tuple);
        } catch (EventHubException ex) {
            collector.reportError(ex);
            collector.fail(tuple);
        } catch (ServiceBusException e) {
            collector.reportError(e);
            collector.fail(tuple);
        }
    }

    @Override
    public void cleanup() {
        if (sender != null) {
            try {
                sender.close().whenComplete((voidargs, error) -> {
                    try {
                        if (error != null) {
                            logger.error("Exception during sender cleanup phase" + error.toString());
                        }
                        ehClient.closeSync();
                    } catch (Exception e) {
                        logger.error("Exception during ehclient cleanup phase" + e.toString());
                    }
                }).get();
            } catch (InterruptedException e) {
                logger.error("Exception occured during cleanup phase" + e.toString());
            } catch (ExecutionException e) {
                logger.error("Exception occured during cleanup phase" + e.toString());
            }
            logger.info("Eventhub Bolt cleaned up");
            sender = null;
            ehClient = null;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
