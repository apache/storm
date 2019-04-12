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
package org.apache.storm.eventhubs.core;

import com.google.common.collect.Iterables;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;

import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EventHubReceiverImpl implements IEventHubReceiver {
    private static final Logger logger = LoggerFactory.getLogger(EventHubReceiverImpl.class);

    private final EventHubConfig eventHubConfig;

    private final String partitionId;
    private PartitionReceiver receiver;
    private EventHubClient ehClient;

    private ReducedMetric receiveApiLatencyMean;
    private CountMetric receiveApiCallCount;
    private CountMetric receiveMessageCount;

    public EventHubReceiverImpl(EventHubConfig config, String partitionId) {
        this.partitionId = partitionId;
        this.eventHubConfig = config;
    
        receiveApiLatencyMean = new ReducedMetric(new MeanReducer());
        receiveApiCallCount = new CountMetric();
        receiveMessageCount = new CountMetric();
    }

    @Override
    public void open(IEventFilter filter) throws ServiceBusException, IOException {
        long start = System.currentTimeMillis();
        logger.debug(String.format("Creating EventHub Client: partitionId: %s, filter value: %s, prefetchCount:%d",
        	    this.partitionId, filter.toString(), this.eventHubConfig.getPrefetchCount()));
    
        this.ehClient = EventHubClient.createFromConnectionStringSync(this.eventHubConfig.getConnectionString());
        this.receiver = PartitionReceiverFactory.createReceiver(this.ehClient, filter, this.eventHubConfig,
                this.partitionId);
        this.receiver.setPrefetchCount(this.eventHubConfig.getPrefetchCount());
        logger.info("created eventhub receiver, time taken(ms): " + (System.currentTimeMillis() - start));
    }

    @Override
    public void close() {
        if (this.receiver != null) {
	        try {
	            this.receiver.closeSync();
	        } catch (ServiceBusException e) {
	    	    logger.error("Exception during receiver close phase " + e.toString());
	        }
	        this.receiver = null;
	    }
		if (this.ehClient != null) {
			try {
				this.ehClient.closeSync();
			} catch (ServiceBusException e) {
				logger.error("Exception during ehclient close phase " + e.toString());
			}
			this.ehClient =  null;
		}
	    logger.info("closed eventhub receiver: partitionId=" + partitionId);
    }


    @Override
    public boolean isOpen() {
        return (this.receiver != null);
    }
  
    @Override
    public Iterable<EventData> receive() {
	    return receive(this.eventHubConfig.getReceiveEventsMaxCount());
    }

    @Override
    public Iterable<EventData> receive(int batchSize) {
        long start = System.currentTimeMillis();
        Iterable<EventData> receivedEvents = null;
    
        try {
            receivedEvents = receiver.receiveSync(batchSize);
            if (receivedEvents != null) {
            	logger.debug("Batchsize: " + batchSize + ", Received event count: " +
            			Iterables.size(receivedEvents));
            }
        } catch (ServiceBusException e) {
            logger.error("Exception occured during receive" + e.toString());
            return null;
        }
        long end = System.currentTimeMillis();
        long millis = (end - start);
        this.receiveApiLatencyMean.update(millis);
        this.receiveApiCallCount.incr();
        return receivedEvents;
    }

    @Override
    public Map<String, Object> getMetricsData() {
        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put(this.partitionId + "/receiveApiLatencyMean", receiveApiLatencyMean.getValueAndReset());
        ret.put(this.partitionId + "/receiveApiCallCount", receiveApiCallCount.getValueAndReset());
        ret.put(this.partitionId + "/receiveMessageCount", receiveMessageCount.getValueAndReset());
        return ret;
    }
}
