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

import java.time.Instant;
import java.util.Map;

import org.apache.storm.eventhubs.state.IStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.microsoft.azure.eventhubs.EventData;

/**
 * A simple partition manager that does not handle/re-send failed messages
 */
public class SimplePartitionManager implements IPartitionManager {
    private static final Logger logger = LoggerFactory.getLogger(SimplePartitionManager.class);
    protected static final String statePathPrefix = "/eventhubspout";

    private EventHubMessage lastEvent = null;
    private String committedOffset = FieldConstants.DefaultStartingOffset;

    protected final IEventHubReceiver receiver;
    protected final EventHubConfig config;
    protected final String partitionId;
    protected final IStateStore stateStore;
    protected final String statePath;
    protected final String partitionPath;

    public SimplePartitionManager(EventHubConfig ehConfig, String partitionId, IStateStore stateStore,
                                  IEventHubReceiver receiver) {
        this.receiver = receiver;
        this.config = ehConfig;
        this.partitionId = partitionId;
        this.statePath = this.getPartitionStatePath();
        this.partitionPath = this.getPartitionPath();
        this.stateStore = stateStore;
    }

    @Override
    public void open() throws Exception {
        // read from state store, if not found, use startingOffset
        final String offset = stateStore.readData(statePath);
        logger.debug("read offset: " + offset + ", from state store: " + this.statePath);

        final IEventFilter filter;
        if (offset == null && config.getEnqueueTimeFilter() != 0) {
            filter = new TimestampFilter(Instant.ofEpochMilli(config.getEnqueueTimeFilter()));
        } else {
            filter = new OffsetFilter((offset == null) ? FieldConstants.DefaultStartingOffset : offset);
        }

        receiver.open(filter);
    }

    @Override
    public void close() {
        this.receiver.close();
        this.checkpoint();
    }

    @Override
    public void checkpoint() {
        final EventHubMessage completedEvent = getCompletedEvent();
        if (completedEvent == null || this.committedOffset.equals(completedEvent.getOffset())) {
            logger.debug("No check-pointing needed. Completed Offset: " +
                    (completedEvent == null ? "null" : completedEvent.getOffset()));
            return;
        }

        logger.info("saving Offset: " + completedEvent.getOffset() + ", to path: "
                + this.statePath + ", at SequenceNo: " + completedEvent.getSequenceNumber());
        stateStore.saveData(this.statePath, completedEvent.getOffset());
        this.committedOffset = completedEvent.getOffset();
    }

    protected EventHubMessage getCompletedEvent() {
        return this.lastEvent;
    }

    @Override
    public EventHubMessage receive() {
        final Iterable<EventData> receivedEvents = receiver.receive(1);
        final EventData receivedEvent = Iterables.getLast(receivedEvents);
        if (receivedEvent != null) {
            this.lastEvent = new EventHubMessage(receivedEvent, partitionId);
            return this.lastEvent;
        }

        return null;
    }

    @Override
    public void ack(String offset) {
    }

    @Override
    public void fail(String offset) {
        logger.warn("ignored fail on offset: " + offset + ", partition: " + this.partitionPath);
    }

    private String getPartitionStatePath() {
        // "/{prefix}/{topologyName}/{namespace}/{entityPath}/partitions/{partitionId}/state";
        final String partitionStatePath = String.join("/", new String[]{statePathPrefix, config.getTopologyName(),
                config.getNamespace(), config.getEntityPath(), "partitions", partitionId});
        logger.debug("partition state path: " + partitionStatePath);
        return partitionStatePath;
    }

    private String getPartitionPath() {
        return String.join("/",
                config.getNamespace(), config.getEntityPath(), config.getConsumerGroupName(), this.partitionId);
    }

    @Override
    public Map<String, Object> getMetricsData() {
        return receiver.getMetricsData();
    }

    @Override
    public String getPartitionId() {
        return partitionId;
    }
}
