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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.PartitionSender;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bolt that writes event message to EventHub.
 * <p>
 * <p>
 * The implementation has two modes of operation:
 * <ul>
 * <li>partitionmode = true, One bolt for per partition write.</li>
 * <li>partitionmode = false, use default partitioning key strategy to write to
 * partition(s)</li>
 * </ul>
 * </p>
 */
public class EventHubBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(EventHubBolt.class);

    private ExecutorService executorService;
    protected OutputCollector collector;
    protected EventHubClient ehClient;
    protected PartitionSender sender;
    protected EventHubBoltConfig boltConfig;

    /**
     * Constructs an instance that uses the specified connection string to connect
     * to an EventHub and write to the specified entityPath
     *
     * @param connectionString EventHub connection String
     * @param entityPath       entity path to write to
     */
    public EventHubBolt(String connectionString, String entityPath) {
        boltConfig = new EventHubBoltConfig(connectionString, entityPath);
    }

    /**
     * Constructs an instance that connects to an EventHub using the specified
     * connection credentials.
     *
     * @param userName      UserName to connect as
     * @param password      Password to use
     * @param namespace     event hub namespace
     * @param entityPath    Name of the event hub
     * @param useTaskIndexAsPartitionId Use TaskIndex from TopologyContext to create Event Hub Sender
     */
    public EventHubBolt(String userName, String password, String namespace, String entityPath, boolean useTaskIndexAsPartitionId) {
        boltConfig = new EventHubBoltConfig(userName, password, namespace, entityPath, useTaskIndexAsPartitionId);
    }

    /**
     * Constructs an instance using the specified configuration
     *
     * @param config EventHub connection and partition configuration
     */
    public EventHubBolt(EventHubBoltConfig config) {
        boltConfig = config;
    }

    @Override
    public void prepare(Map<String, Object> config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        logger.info(String.format("Connection String: %s, PartitionMode: %s",
                removeSecretFromConnectionString(boltConfig.getConnectionString()),
                String.valueOf(boltConfig.getUseTaskIndexAsPartitionId())));
        try {
            executorService = Executors.newSingleThreadExecutor();
            ehClient = EventHubClient.createSync(boltConfig.getConnectionString(), executorService);
            if (boltConfig.getUseTaskIndexAsPartitionId()) {
                final String partitionId = String.valueOf(context.getThisTaskIndex());
                logger.info("Writing to partition id: " + partitionId);
                sender = ehClient.createPartitionSenderSync(partitionId);
            }
        } catch (Exception ex) {
            collector.reportError(ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            final EventData sendEvent = EventData.create(boltConfig.getEventDataFormat().serialize(tuple));
            if (sender == null) {
                ehClient.sendSync(sendEvent);
            } else {
                sender.sendSync(sendEvent);
            }
            collector.ack(tuple);
        } catch (EventHubException e) {
            collector.reportError(e);
            collector.fail(tuple);
        }
    }

    @Override
    public void cleanup() {
        logger.debug("EventHubBolt cleanup");

        if (sender != null) {
            try {
                sender.closeSync();
            } catch (EventHubException e) {
                logger.error("Exception occurred while cleaning up sender" + e.toString());
            } finally {
                sender = null;
            }
        }

        if (ehClient != null) {
            try {
                ehClient.closeSync();
            } catch (EventHubException e) {
                logger.error("Exception occurred during EventHubClient cleanup phase" + e.toString());
            } finally {
                ehClient = null;
            }
        }

        if (executorService != null) {
            executorService.shutdown();

            try {
                executorService.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.warn("Exception occurred while terminating ExecutorService: " + e.toString());
            } finally {
                executorService = null;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    String removeSecretFromConnectionString(final String connectionString) {
        final ConnectionStringBuilder newConnectionStringBuilder = new ConnectionStringBuilder(connectionString);
        if (newConnectionStringBuilder.getSasKeyName() != null || !newConnectionStringBuilder.getSasKeyName().isEmpty()) {
            newConnectionStringBuilder.setSasKeyName("--removing_for_log--");
        }

        if (newConnectionStringBuilder.getSharedAccessSignature() != null || !newConnectionStringBuilder.getSharedAccessSignature().isEmpty()) {
            newConnectionStringBuilder.setSharedAccessSignature("--removing_for_log--");
        }

        return newConnectionStringBuilder.toString();
    }
}
