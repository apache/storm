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

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeSet;

import org.apache.storm.eventhubs.state.IStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.eventhubs.EventData;

public class PartitionManager extends SimplePartitionManager {
    private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);

    // all sent events are stored in pendingAcks
    private final Map<String, EventHubMessage> pendingAcks;

    // all failed events are tracked in failed, & is sorted by event's offset
    private final TreeSet<EventHubMessage> failed;
    private final StreamCheckpointTracker checkpointTracker;
    private final FirstEventTracker firstEventTracker;

    public PartitionManager(EventHubConfig ehConfig,
                            String partitionId,
                            IStateStore stateStore,
                            IEventHubReceiver receiver) {
        super(ehConfig, partitionId, stateStore, receiver);

        this.pendingAcks = new LinkedHashMap<String, EventHubMessage>();
        this.failed = new TreeSet<EventHubMessage>();
        this.checkpointTracker = new StreamCheckpointTracker();
        this.firstEventTracker = new FirstEventTracker();
    }

    @Override
    public EventHubMessage receive() {
        logger.debug("Retrieving messages for partition: " + partitionId);

        if (pendingAcks.size() >= config.getMaxPendingMsgsPerPartition()) {
            logger.debug(String.format("Pending queue full(" + config.getMaxPendingMsgsPerPartition()
                    + ") - no new events will be received from EventHub Partition: %s.", this.partitionId));
            return null;
        }

        final EventHubMessage ehm;
        if (!failed.isEmpty()) {
            ehm = failed.pollFirst();
        } else {
            final Iterable<EventData> events = this.receiver.receive(1);
            ehm = (events == null || !events.iterator().hasNext())
                    ? null
                    : new EventHubMessage(events.iterator().next(), this.partitionId);
        }

        if (ehm == null) {
            logger.debug(
                    String.format("No messages available from EventHubPartition: %s, or waiting for reprocessing.",
                    this.partitionId));
            return null;
        }

        this.firstEventTracker.set(ehm);
        pendingAcks.put(ehm.getOffset(), ehm);

        return ehm;
    }

    @Override
    public void ack(String offset) {
        final EventHubMessage ackedMessage = pendingAcks.remove(offset);
        if (this.firstEventTracker.isFirstEvent(offset)) {
            this.checkpointTracker.setFirstCheckpoint(ackedMessage);
        } else {
            this.checkpointTracker.add(ackedMessage);
        }
    }

    @Override
    public void fail(String offset) {
        logger.warn(String.format("fail on offset: %s, for partition: %s", offset, this.partitionId));
        failed.add(pendingAcks.remove(offset));
    }

    @Override
    protected String getCompletedOffset() {
        return this.checkpointTracker.getCheckpoint();
    }

    static class StreamCheckpointTracker {
        private final PriorityQueue<EventHubMessage> minHeap;
        private volatile EventHubMessage nextCheckpoint;

        public StreamCheckpointTracker() {
            this.nextCheckpoint = null;
            this.minHeap = new PriorityQueue<>(new Comparator<EventHubMessage>() {
                @Override
                public int compare(final EventHubMessage ehm1, final EventHubMessage ehm2) {
                    return (int) (ehm1.getSequenceNumber() - ehm2.getSequenceNumber());
                }
            });
        }

        public void setFirstCheckpoint(final EventHubMessage eventHubMessage) {
            if (this.nextCheckpoint != null) {
                throw new IllegalStateException("first checkpoint should be set only once");
            }

            this.nextCheckpoint = eventHubMessage;
        }

        public void add(final EventHubMessage eventHubMessage) {
            this.minHeap.add(eventHubMessage);

            if (nextCheckpoint != null) {
                while (!minHeap.isEmpty()
                        && minHeap.peek().getSequenceNumber() == nextCheckpoint.getSequenceNumber() + 1) {
                    this.nextCheckpoint = minHeap.poll();
                }
            }
        }

        public String getCheckpoint() {
            return this.nextCheckpoint == null
                    ? null
                    : this.nextCheckpoint.getOffset();
        }
    }

    static class FirstEventTracker {
        private volatile boolean isFirstEventSet;
        private volatile boolean isFirstEventMatched;
        private volatile EventHubMessage firstEvent;

        public FirstEventTracker() {
            this.isFirstEventSet = false;
            this.isFirstEventMatched = false;
            this.firstEvent = null;
        }

        public void set(final EventHubMessage firstEvent) {
            if (!this.isFirstEventSet) {
                this.firstEvent = firstEvent;
                this.isFirstEventSet = true;
            }
        }

        public boolean isFirstEvent(final String offset) {
            if (this.isFirstEventMatched) {
                return false;
            }

            if (this.firstEvent != null
                    && this.firstEvent.getOffset().compareToIgnoreCase(offset) == 0) {
                this.isFirstEventMatched = true;
                return true;
            }

            return false;
        }
    }
}
