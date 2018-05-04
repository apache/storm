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
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.EventPosition;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.ReceiverOptions;

public final class PartitionReceiverFactory {


    public static PartitionReceiver createReceiver(EventHubClient ehClient, IEventFilter filter,
                                                   EventHubConfig eventHubConfig, String partitionId) throws EventHubException {

        if (filter instanceof OffsetFilter) {
            return createOffsetReceiver(ehClient, (OffsetFilter) filter, eventHubConfig, partitionId);
        } else {
            return createTimestampReceiver(ehClient, (TimestampFilter) filter, eventHubConfig, partitionId);
        }
    }

    private static PartitionReceiver createOffsetReceiver(EventHubClient ehClient, OffsetFilter filter,
                                                          EventHubConfig eventHubConfig, String partitionId) throws EventHubException {
        final ReceiverOptions options = new ReceiverOptions();
        options.setReceiverRuntimeMetricEnabled(true);
        return ehClient.createEpochReceiverSync(
                eventHubConfig.getConsumerGroupName(),
                partitionId,
                EventPosition.fromOffset(filter.getOffset(), false),
                1,
                options);
    }

    private static PartitionReceiver createTimestampReceiver(EventHubClient ehClient, TimestampFilter filter,
                                                             EventHubConfig eventHubConfig, String partitionId) throws EventHubException {

        final ReceiverOptions options = new ReceiverOptions();
        options.setReceiverRuntimeMetricEnabled(true);
        return ehClient.createEpochReceiverSync(
                eventHubConfig.getConsumerGroupName(),
                partitionId,
                EventPosition.fromEnqueuedTime(filter.getTime()),
                1,
                options);
    }
}
