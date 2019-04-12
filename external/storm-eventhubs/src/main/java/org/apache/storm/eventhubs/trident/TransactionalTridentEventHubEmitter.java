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
package org.apache.storm.eventhubs.trident;

import org.apache.storm.eventhubs.core.EventHubReceiverImpl;
import org.apache.storm.eventhubs.core.FieldConstants;
import org.apache.storm.eventhubs.core.EventHubConfig;
import org.apache.storm.eventhubs.core.EventHubMessage;
import org.apache.storm.eventhubs.core.IEventHubReceiver;
import org.apache.storm.eventhubs.core.IEventHubReceiverFactory;
import org.apache.storm.eventhubs.core.Partition;
import org.apache.storm.eventhubs.core.Partitions;
import org.apache.storm.eventhubs.spout.*;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.servicebus.ServiceBusException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TransactionalTridentEventHubEmitter
    implements IPartitionedTridentSpout.Emitter<Partitions, Partition, Map<String, String>> {
  private static final Logger logger = LoggerFactory.getLogger(TransactionalTridentEventHubEmitter.class);
  private final int batchSize; 
  private final EventHubSpoutConfig spoutConfig;
  private Map<String, ITridentPartitionManager> pmMap;
  private ITridentPartitionManagerFactory pmFactory;
  private IEventHubReceiverFactory recvFactory;
  private final String OFFSET_KEY = "offset";
  private final String NEXT_OFFSET_KEY = "nextOffset";
  private final String COUNT_KEY = "count";
  
  public TransactionalTridentEventHubEmitter(EventHubSpoutConfig spoutConfig) {
    //use batch size that matches the default credit size
    this(spoutConfig, spoutConfig.getReceiverCredits(), null, null);
  }
      
  public TransactionalTridentEventHubEmitter(final EventHubSpoutConfig spoutConfig, int batchSize,
      ITridentPartitionManagerFactory pmFactory, IEventHubReceiverFactory recvFactory) {
    this.spoutConfig = spoutConfig;
    this.batchSize = batchSize;
    this.pmFactory = pmFactory;
    this.recvFactory = recvFactory;
    this.pmMap = new HashMap<String, ITridentPartitionManager>();
    
    if (this.pmFactory == null) {
      this.pmFactory = new ITridentPartitionManagerFactory() {
    	  private static final long serialVersionUID = 7694972161358936580L;
    	  
        @Override
        public ITridentPartitionManager create(IEventHubReceiver receiver, String partitionId) {
          return new TridentPartitionManager(spoutConfig, receiver, partitionId);
        }
      };
    }
    
    if (this.recvFactory == null) {
      this.recvFactory = new IEventHubReceiverFactory() {
    	  private static final long serialVersionUID = 5105886777136979123L;
    	  
        @Override
        public IEventHubReceiver create(EventHubConfig config, String partitionId) {
          return new EventHubReceiverImpl(config, partitionId);
        }
      };
    }
  }
  
  @Override
  public void close() {
    for (ITridentPartitionManager pm: pmMap.values()) {
      pm.close();
    }
  }
  
  /**
   * Check if partition manager for a given partiton is created
   * if not, create it.
   * @param partition
   */
  private ITridentPartitionManager getOrCreatePartitionManager(Partition partition) {
    ITridentPartitionManager pm;
    if(!this.pmMap.containsKey(partition.getId())) {
      IEventHubReceiver receiver = this.recvFactory.create(this.spoutConfig, partition.getId());
      pm = this.pmFactory.create(receiver, partition.getId());
      this.pmMap.put(partition.getId(), pm);
    }
    else {
      pm = this.pmMap.get(partition.getId());
    }
    return pm;
  }

  @Override
  public void emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, Partition partition,
		  Map<String, String> meta) {
    String offset = meta.get("offset");
    int count = Integer.parseInt(meta.get("count"));
    logger.info("re-emit for partition " + partition.getId() + ", offset=" + offset + ", count=" + count);
    ITridentPartitionManager pm = getOrCreatePartitionManager(partition);
    List<EventHubMessage> listEvents = null;
    try {
    	listEvents = pm.receiveBatch(offset, count);
    } catch (IOException | ServiceBusException e) {
    	throw new RuntimeException("Failed to retrieve events from EventHub.", e);
    }
    if (listEvents.size() != count) {
      logger.error("failed to refetch eventhub messages, new count=" + listEvents.size());
      return;
    }

    for (EventHubMessage ed : listEvents) {
      List<Object> tuples = this.spoutConfig.getEventDataScheme().deserialize(ed);
      collector.emit(tuples);
    }
  }

  @Override
  public Map<String, String> emitPartitionBatchNew(TransactionAttempt attempt, TridentCollector collector,
		  Partition partition, Map<String, String> meta) {
    ITridentPartitionManager pm = getOrCreatePartitionManager(partition);
    String offset = (meta != null) ? meta.getOrDefault(NEXT_OFFSET_KEY, FieldConstants.DefaultStartingOffset) :
    	FieldConstants.DefaultStartingOffset;
    String nextOffset = offset;

    List<EventHubMessage> listEvents = null;
    try {
    	listEvents = pm.receiveBatch(offset, batchSize);
    } catch (IOException | ServiceBusException e) {
    	throw new RuntimeException("Failed to retrieve events from EventHub.", e);
    }

    for (EventHubMessage ed : listEvents) {
      // update nextOffset;
      nextOffset = ed.getMessageId().getOffset();
      List<Object> tuples = this.spoutConfig.getEventDataScheme().deserialize(ed);
      collector.emit(tuples);
    }
    
    Map<String, String> newMeta = new HashMap<String, String>();
    newMeta.put(OFFSET_KEY, offset);
    newMeta.put(NEXT_OFFSET_KEY, nextOffset);
    newMeta.put(COUNT_KEY, String.valueOf(listEvents.size()));
    return newMeta;
  }

  @Override
  public List<Partition> getOrderedPartitions(Partitions partitions) {
    return partitions.getPartitions();
  }

  @Override
  public void refreshPartitions(List<Partition> partitionList) {
    //partition info does not change in EventHub
  }
}
