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

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;

public final class PartitionReceiverFactory {
    public static PartitionReceiver createReceiver(EventHubClient ehClient, IEventFilter filter,
            EventHubConfig eventHubConfig, String partitionId) throws ServiceBusException
    {
        if ((filter instanceof OffsetFilter)) {
            return createOffsetReceiver(ehClient, (OffsetFilter)filter, eventHubConfig, partitionId);
        }
        return createTimestampReceiver(ehClient, (TimestampFilter)filter, eventHubConfig, partitionId);
    }
			  
    private static PartitionReceiver createOffsetReceiver(EventHubClient ehClient, OffsetFilter filter,
            EventHubConfig eventHubConfig, String partitionId) throws ServiceBusException
    {
        return ehClient.createEpochReceiverSync(eventHubConfig.getConsumerGroupName(), partitionId,
                filter.getOffset(), false, 1L);
    }
			  
    private static PartitionReceiver createTimestampReceiver(EventHubClient ehClient, TimestampFilter filter,
            EventHubConfig eventHubConfig, String partitionId) throws ServiceBusException
    {
        return ehClient.createEpochReceiverSync(eventHubConfig.getConsumerGroupName(), partitionId,
                filter.getTime(), 1L);
    }
}
