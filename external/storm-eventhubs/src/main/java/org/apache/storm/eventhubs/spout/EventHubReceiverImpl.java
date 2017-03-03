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

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;

import com.microsoft.eventhubs.client.Constants;
import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.azure.eventhubs.EventData;

import java.util.concurrent.ExecutionException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventHubReceiverImpl implements IEventHubReceiver {
  private static final Logger logger = LoggerFactory.getLogger(EventHubReceiverImpl.class);

  private final String connectionString;
  private final String entityName;
  private final String partitionId;
  private final String consumerGroupName;

  private PartitionReceiver receiver;
  private EventHubClient ehClient=null;
  private ReducedMetric receiveApiLatencyMean;
  private CountMetric receiveApiCallCount;
  private CountMetric receiveMessageCount;

  public EventHubReceiverImpl(EventHubSpoutConfig config, String partitionId) {
    this.connectionString = config.getConnectionString();
    this.entityName = config.getEntityPath();
    this.partitionId = partitionId;
    this.consumerGroupName = config.getConsumerGroupName();
    receiveApiLatencyMean = new ReducedMetric(new MeanReducer());
    receiveApiCallCount = new CountMetric();
    receiveMessageCount = new CountMetric();
  }

  @Override
  public void open(String offset) throws EventHubException {
    logger.info("creating eventhub receiver: partitionId=" + partitionId +
            ", offset=" + offset);
    long start = System.currentTimeMillis();
    try {
      ehClient = EventHubClient.createFromConnectionStringSync(connectionString);
      receiver = ehClient.createEpochReceiverSync(
              consumerGroupName,
              partitionId,
              offset,
              false,
              1);
    }catch (Exception e){
      logger.info("Exception in creating EventhubClient"+e.toString());
    }
    long end = System.currentTimeMillis();
    logger.info("created eventhub receiver, time taken(ms): " + (end-start));
  }

  @Override
  public void close(){
    if(receiver != null) {
      try {
        receiver.close().whenComplete((voidargs,error)->{
          try{
            if(error!=null){
              logger.error("Exception during receiver close phase"+error.toString());
            }
            ehClient.closeSync();
          }catch (Exception e){
            logger.error("Exception during ehclient close phase"+e.toString());
          }
        }).get();
      }catch (InterruptedException e){
        logger.error("Exception occured during close phase"+e.toString());
      }catch (ExecutionException e){
        logger.error("Exception occured during close phase"+e.toString());
      }
      logger.info("closed eventhub receiver: partitionId=" + partitionId );
      receiver = null;
      ehClient =  null;
    }
  }

  
  @Override
  public boolean isOpen() {
    return (receiver != null);
  }

  @Override
  public EventDataWrap receive() {
    long start = System.currentTimeMillis();
    Iterable<EventData> receivedEvents=null;
    /*Get one message at a time for backward compatibility behaviour*/
    try {
      receivedEvents = receiver.receiveSync(1);
    }catch (Exception e){
      logger.error("Exception occured during receive"+e.toString());
    }
    long end = System.currentTimeMillis();
    long millis = (end - start);
    receiveApiLatencyMean.update(millis);
    receiveApiCallCount.incr();
    if (receivedEvents == null) {
      return null;
    }
    receiveMessageCount.incr();
    EventData receivedEvent = receivedEvents.iterator().next();
    MessageId messageId = new MessageId(partitionId,
            receivedEvent.getSystemProperties().getOffset(),
            receivedEvent.getSystemProperties().getSequenceNumber());

    return EventDataWrap.create(receivedEvent,messageId);
  }

  @Override
  public Map getMetricsData() {
    Map ret = new HashMap();
    ret.put(partitionId + "/receiveApiLatencyMean", receiveApiLatencyMean.getValueAndReset());
    ret.put(partitionId + "/receiveApiCallCount", receiveApiCallCount.getValueAndReset());
    ret.put(partitionId + "/receiveMessageCount", receiveMessageCount.getValueAndReset());
    return ret;
  }
}
