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

import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.eventhubs.spout.IEventHubReceiverFactory;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A thin wrapper of TransactionalTridentEventHubEmitter for OpaqueTridentEventHubSpout
 */
public class OpaqueTridentEventHubEmitter implements IOpaquePartitionedTridentSpout.Emitter<Partitions, Partition, Map> {
  private final TransactionalTridentEventHubEmitter transactionalEmitter;
  public OpaqueTridentEventHubEmitter(EventHubSpoutConfig spoutConfig) {
    transactionalEmitter = new TransactionalTridentEventHubEmitter(spoutConfig);
  }
  
  public OpaqueTridentEventHubEmitter(EventHubSpoutConfig spoutConfig,
      int batchSize,
      ITridentPartitionManagerFactory pmFactory,
      IEventHubReceiverFactory recvFactory) {
    transactionalEmitter = new TransactionalTridentEventHubEmitter(spoutConfig,
        batchSize,
        pmFactory,
        recvFactory);
  }

  @Override
  public void close() {
    transactionalEmitter.close();
  }

  @Override
  public Map emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector,
      Partition partition, Map meta) {
    return transactionalEmitter.emitPartitionBatchNew(attempt, collector, partition, meta);
  }

  @Override
  public List<Partition> getOrderedPartitions(Partitions partitions) {
    return transactionalEmitter.getOrderedPartitions(partitions);
  }

  @Override
  public List<Partition> getPartitionsForTask(int taskId, int numTasks, Partitions allPartitionInfo) {
    final List<Partition> orderedPartitions = getOrderedPartitions(allPartitionInfo);
    final List<Partition> taskPartitions = new ArrayList<>(orderedPartitions == null ? 0 : orderedPartitions.size());
    if (orderedPartitions != null) {
      for (int i = taskId; i < orderedPartitions.size(); i += numTasks) {
        taskPartitions.add(orderedPartitions.get(i));
      }
    }
    return taskPartitions;
  }

  @Override
  public void refreshPartitions(List<Partition> partitionList) {
    transactionalEmitter.refreshPartitions(partitionList);
  }
}
