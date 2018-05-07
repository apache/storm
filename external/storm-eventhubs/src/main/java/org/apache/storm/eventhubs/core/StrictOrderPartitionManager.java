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
import org.apache.storm.eventhubs.state.IStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.microsoft.azure.eventhubs.EventData;

/**
 * Strict Order Partition Manager. Emits only 1 event at-a-time per partition.
 * Use this when Order guarantees across events in a single partition
 * can trump the performance of the pipeline.
 * Waits for the ack/fail - before returning next event.
 * If SPOUT reports failure on the event - it will re-emit until MAX_RETRY.
 */
public class StrictOrderPartitionManager extends SimplePartitionManager {
    private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);
    private final int maxRetryCount = 10;

    private Object messageExchangeLock;

    private EventHubMessage lastAckedMessage;
    private EventHubMessage lastEmittedMessage;
    private EventHubMessage lastFailedMessage;

    private int currentRetryCount;

    public StrictOrderPartitionManager(EventHubConfig ehConfig, String partitionId, IStateStore stateStore,
                            IEventHubReceiver receiver) {
        super(ehConfig, partitionId, stateStore, receiver);

        this.currentRetryCount = 0;
        this.messageExchangeLock = new Object();
    }

    @Override
    public EventHubMessage receive() {
        logger.debug("Retrieving message from partition: " + partitionId);

        synchronized (this.messageExchangeLock) {
            if (this.lastEmittedMessage != null) {
                Log.debug(
                        "There is 1 pending event which is not ACKed. No new events will be retrieved from EventHub Partition: "
                                + this.partitionId);
                return null;
            }

            if (this.lastFailedMessage != null) {
                this.lastEmittedMessage = this.lastFailedMessage;
                this.lastFailedMessage = null;
                this.currentRetryCount++;
                return this.lastEmittedMessage;
            }

            final Iterable<EventData> receivedEvents = receiver.receive(1);
            if (receivedEvents == null || !receivedEvents.iterator().hasNext()) {
                logger.debug("No messages received from EventHub, PartitionId: " + partitionId);
                return null;
            }

            final EventHubMessage ehm = new EventHubMessage(Iterables.getLast(receivedEvents), partitionId);
            this.lastEmittedMessage = ehm;
            return ehm;
        }
    }

    @Override
    public void ack(String offset) {
        synchronized (this.messageExchangeLock) {
            if (!this.lastEmittedMessage.getOffset().equalsIgnoreCase(offset)) {
                throw new RuntimeException(
                        String.format(
                                "InconsistentState: Offset ACKed by Storm (%s) & emitted by SPOUT (%s) were different.",
                                offset,
                                this.lastEmittedMessage.getOffset()));
            }

            this.lastAckedMessage = this.lastEmittedMessage;
            this.currentRetryCount = 0;
            this.lastEmittedMessage = null;
        }
    }

    @Override
    public void fail(String offset) {
        synchronized (this.messageExchangeLock) {
            logger.warn(String.format("fail on offset: %s, partitionId: %s", offset, this.partitionId));
            if (!this.lastEmittedMessage.getOffset().equalsIgnoreCase(offset)) {
                throw new RuntimeException(
                        String.format(
                                "InconsistentState: Offset FAILed by Storm (%s) & emitted by SPOUT (%s) were different.",
                                offset,
                                this.lastEmittedMessage.getOffset()));
            }

            if (this.currentRetryCount > maxRetryCount) {
                throw new RuntimeException(
                        String.format("Even after %s retries, processing the offset: %s did not succeed.",
                                maxRetryCount,
                                offset));
            }

            this.lastFailedMessage = this.lastEmittedMessage;
            this.lastEmittedMessage = null;
        }
    }

    @Override
    protected String getCompletedOffset() {
        synchronized (this.messageExchangeLock) {
            return this.lastAckedMessage != null
                    ? this.lastAckedMessage.getOffset()
                    : FieldConstants.DefaultStartingOffset;
        }
    }
}
