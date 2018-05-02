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

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import org.apache.storm.eventhubs.format.SerializeDeserializeUtil;

import com.microsoft.azure.eventhubs.EventData;

/**
 * Represents a message from EventHub. Encapsulates the actual pay load received
 * from EventHub.
 * <p>
 * It encapsulates the raw bytes from the content, any AMQP application
 * properties set, and the system properties (partition key, offset, enqueue
 * time, sequence number, and publisher) set on the Eventhub message.
 */
public class EventHubMessage implements Comparable<EventHubMessage>, Serializable {
    private final byte[] content;
    private final String partitionId;
    private final String partitionKey;
    private final String offset;
    private final Instant enqueuedTime;
    private final long sequenceNumber;
    private final String publisher;
    private final MessageId messageId;

    private final Map<String, Object> applicationProperties;
    private final Map<String, Object> systemProperties;

    public EventHubMessage(EventData eventdata, String partitionId) {
        this.partitionId = partitionId;

        if (eventdata.getBytes() != null) {
            content = eventdata.getBytes();
        } else if (eventdata.getObject() != null) {
            try {
                content = SerializeDeserializeUtil.serialize(eventdata.getObject());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("Failed to retrieve payload from EventData");
        }

        applicationProperties = eventdata.getProperties();
        systemProperties = eventdata.getSystemProperties();

        if (eventdata.getSystemProperties() != null) {
            offset = eventdata.getSystemProperties().getOffset();
            partitionKey = eventdata.getSystemProperties().getPartitionKey();
            enqueuedTime = eventdata.getSystemProperties().getEnqueuedTime();
            sequenceNumber = eventdata.getSystemProperties().getSequenceNumber();
            publisher = eventdata.getSystemProperties().getPublisher();
        } else {
            this.offset = null;
            this.partitionKey = null;
            this.enqueuedTime = null;
            this.sequenceNumber = FieldConstants.DEFAULT_SEQUENCE_NUMBER;
            this.publisher = null;
        }

        messageId = new MessageId(partitionId, offset, sequenceNumber);
    }

    public String readAsString() {
        return readAsString(Charset.defaultCharset());
    }

    public String readAsAsciiString() {
        return readAsString(StandardCharsets.US_ASCII);
    }

    public String readAsUtf8String() {
        return readAsString(StandardCharsets.UTF_8);
    }

    public String readAsUtf16String() {
        return readAsString(StandardCharsets.UTF_16);
    }

    public String readAsString(Charset charset) {
        return new String(content, charset);
    }

    public byte[] getContent() {
        return this.content;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public String getOffset() {
        return offset;
    }

    public Instant getEnqueuedTime() {
        return enqueuedTime;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getPublisher() {
        return publisher;
    }

    public Map<String, Object> getApplicationProperties() {
        return applicationProperties;
    }

    public Map<String, Object> getSystemProperties() {
        return systemProperties;
    }

    public MessageId getMessageId() {
        return messageId;
    }

    @Override
    public int compareTo(EventHubMessage o) {
        return this.getSequenceNumber().compareTo(o.getSequenceNumber());
    }
}
