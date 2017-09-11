package org.apache.storm.eventhubs.core;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;

public final class PartitionReceiverFactory {

	public static PartitionReceiver createReceiver(EventHubClient ehClient, IEventFilter filter,
			EventHubConfig eventHubConfig, String partitionId) throws ServiceBusException {

		if (filter instanceof OffsetFilter) {
			return createOffsetReceiver(ehClient, (OffsetFilter) filter, eventHubConfig, partitionId);
		} else {
			return createTimestampReceiver(ehClient, (TimestampFilter) filter, eventHubConfig, partitionId);
		}
	}

	private static PartitionReceiver createOffsetReceiver(EventHubClient ehClient, OffsetFilter filter,
			EventHubConfig eventHubConfig, String partitionId) throws ServiceBusException {

		return ehClient.createEpochReceiverSync(eventHubConfig.getConsumerGroupName(), partitionId, filter.getOffset(),
				false, 1);
	}

	private static PartitionReceiver createTimestampReceiver(EventHubClient ehClient, TimestampFilter filter,
			EventHubConfig eventHubConfig, String partitionId) throws ServiceBusException {

		return ehClient.createEpochReceiverSync(eventHubConfig.getConsumerGroupName(), partitionId, filter.getTime(),
				1);
	}
}
