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

package org.apache.storm.eventhubs.spout;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHubReceiverImpl implements IEventHubReceiver {
    private static final Logger logger = LoggerFactory.getLogger(EventHubReceiverImpl.class);

    private final String connectionString;
    private final String entityName;
    private final String partitionId;
    private final String consumerGroupName;
    private final int receiverTimeoutInMillis;

    private PartitionReceiver receiver;
    private EventHubClient ehClient;
    private ReducedMetric receiveApiLatencyMean;
    private CountMetric receiveApiCallCount;
    private CountMetric receiveMessageCount;

    public EventHubReceiverImpl(EventHubSpoutConfig config, String partitionId) {
        this.connectionString = config.getConnectionString();
        this.entityName = config.getEntityPath();
        this.partitionId = partitionId;
        this.consumerGroupName = config.getConsumerGroupName();
        this.receiverTimeoutInMillis = config.getReceiverTimeoutInMillis();
        receiveApiLatencyMean = new ReducedMetric(new MeanReducer());
        receiveApiCallCount = new CountMetric();
        receiveMessageCount = new CountMetric();
    }

    @Override
    public void open(IEventFilter filter) throws EventHubException {
        logger.info("creating eventhub receiver: partitionId=" + partitionId
                + ", filter=" + filter.getOffset() != null
                ? filter.getOffset()
                : Long.toString(filter.getTime().toEpochMilli()));
        long start = System.currentTimeMillis();
        try {
            ehClient = EventHubClient.createFromConnectionStringSync(connectionString);

            if (filter.getOffset() != null) {
                receiver = ehClient.createEpochReceiverSync(
                    consumerGroupName,
                    partitionId,
                    filter.getOffset(),
                    false,
                    1);
            } else if (filter.getTime() != null) {
                receiver = ehClient.createEpochReceiverSync(
                    consumerGroupName,
                    partitionId,
                    filter.getTime(),
                    1);
            } else {
                throw new RuntimeException("Eventhub receiver must have an offset or time to be created");
            }
            if (receiver != null) {
                receiver.setReceiveTimeout(Duration.ofMillis(receiverTimeoutInMillis));
            }
        } catch (IOException e) {
            logger.error("Exception in creating ehclient" + e.toString());
            throw new EventHubException(e);
        } catch (ServiceBusException e) {
            logger.error("Exception in creating Receiver" + e.toString());
            throw new EventHubException(e);
        }
        long end = System.currentTimeMillis();
        logger.info("created eventhub receiver, time taken(ms): " + (end - start));
    }

    @Override
    public void close() {
        if (receiver != null) {
            try {
                receiver.close().whenComplete((voidargs, error) -> {
                    try {
                        if (error != null) {
                            logger.error("Exception during receiver close phase" + error.toString());
                        }
                        ehClient.closeSync();
                    } catch (Exception e) {
                        logger.error("Exception during ehclient close phase" + e.toString());
                    }
                }).get();
            } catch (InterruptedException e) {
                logger.error("Exception occured during close phase" + e.toString());
            } catch (ExecutionException e) {
                logger.error("Exception occured during close phase" + e.toString());
            }
            logger.info("closed eventhub receiver: partitionId=" + partitionId);
            receiver = null;
            ehClient = null;
        }
    }


    @Override
    public boolean isOpen() {
        return (receiver != null);
    }

    @Override
    public EventDataWrap receive() {
        long start = System.currentTimeMillis();
        Iterable<EventData> receivedEvents = null;
        /*Get one message at a time for backward compatibility behaviour*/
        try {
            receivedEvents = receiver.receiveSync(1);
        } catch (ServiceBusException e) {
            logger.error("Exception occured during receive" + e.toString());
            return null;
        }
        long end = System.currentTimeMillis();
        long millis = (end - start);
        receiveApiLatencyMean.update(millis);
        receiveApiCallCount.incr();

        if (receivedEvents == null || receivedEvents.spliterator().getExactSizeIfKnown() == 0) {
            return null;
        }
        receiveMessageCount.incr();
        EventData receivedEvent = receivedEvents.iterator().next();
        MessageId messageId = new MessageId(partitionId,
                                            receivedEvent.getSystemProperties().getOffset(),
                                            receivedEvent.getSystemProperties().getSequenceNumber());

        return EventDataWrap.create(receivedEvent, messageId);
    }

    @Override
    public Map<String, Object> getMetricsData() {
        Map<String, Object> ret = new HashMap<>();
        ret.put(partitionId + "/receiveApiLatencyMean", receiveApiLatencyMean.getValueAndReset());
        ret.put(partitionId + "/receiveApiCallCount", receiveApiCallCount.getValueAndReset());
        ret.put(partitionId + "/receiveMessageCount", receiveMessageCount.getValueAndReset());
        return ret;
    }
}
