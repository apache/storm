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
package org.apache.storm.eventhubs.bolt;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.storm.eventhubs.core.FieldConstants;
import org.apache.storm.eventhubs.format.DefaultEventDataFormat;
import org.apache.storm.eventhubs.format.IEventDataFormat;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;

/**
 * EventHubs bolt configurations
 * <p>
 * Partition mode: useTaskIndexAsPartitionId=true, in this mode each bolt task will write to
 * a partition with the same id as that of the task index. For this mode, the
 * number of bolt tasks must match the number of partitions.
 * <p>
 * useTaskIndexAsPartitionId=false, default setting. There is no affinity between bolt tasks
 * and partitions. Events are written to partitions as determined by the
 * EventHub partitioning logic.
 *
 * @see IEventDataFormat
 */
public class EventHubBoltConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String connectionString;
    protected boolean useTaskIndexAsPartitionId;
    protected IEventDataFormat dataFormat;

    /**
     * Constructs an instance with specified connection string, and eventhub name
     * The @link {@link #useTaskIndexAsPartitionId} is set to false.
     *
     * @param connectionString EventHub connection string
     * @param entityPath       EventHub name
     */
    public EventHubBoltConfig(final String connectionString, final String entityPath) {
        this(connectionString, entityPath, false, new DefaultEventDataFormat());
    }

    /**
     * Constructs an instance with specified connection string, eventhub name and
     * partition mode.
     *
     * @param connectionString EventHub connection string
     * @param entityPath       EventHub name
     * @param useTaskIndexAsPartitionId    if true will write to specific partition, uses TaskIndex from TopologyContext value to create Sender to Event Hub Partition
     */
    public EventHubBoltConfig(String connectionString, String entityPath, boolean useTaskIndexAsPartitionId) {
        this(connectionString, entityPath, useTaskIndexAsPartitionId, new DefaultEventDataFormat());
    }

    /**
     * Constructs an instance with specified credentials, eventhub name and
     * partition mode.
     * <p>
     * <p>
     * For sovereign clouds please use the constructor
     * {@link EventHubBolt#EventHubBolt(String, String)}.
     * </p>
     *
     * @param userName      user name to connect as
     * @param password      password for the user name
     * @param namespace     Event Hub namespace
     * @param entityPath    EntityHub name
     * @param useTaskIndexAsPartitionId if true will write to specific partition, uses TaskIndex from TopologyContext value to create Sender to Event Hub Partition
     */
    public EventHubBoltConfig(String userName, String password, String namespace, String entityPath,
                              boolean useTaskIndexAsPartitionId) {
        this(userName, password, namespace, FieldConstants.EH_SERVICE_FQDN_SUFFIX, entityPath, useTaskIndexAsPartitionId);
    }

    /**
     * Constructs an instance with specified connection string, and partition mode.
     * The specified {@link IEventDataFormat} will be used to format data to bytes
     * while constructing {@link com.microsoft.azure.eventhubs.EventData#create(byte[])}.
     *
     * @param connectionString EventHub connection string
     * @param useTaskIndexAsPartitionId    if true will write to specific partition, uses TaskIndex from TopologyContext value to create Sender to Event Hub Partition
     * @param dataFormat       data formatter for serializing event data
     */
    public EventHubBoltConfig(String connectionString, String entityPath, boolean useTaskIndexAsPartitionId, IEventDataFormat dataFormat) {
        this.connectionString = new ConnectionStringBuilder(connectionString)
                .setEventHubName(entityPath)
                .toString();
        this.useTaskIndexAsPartitionId = useTaskIndexAsPartitionId;
        this.dataFormat = dataFormat;
        if (this.dataFormat == null) {
            this.dataFormat = new DefaultEventDataFormat();
        }
    }

    /**
     * Constructs an instance with specified credentials, and connection information
     *
     * @param userName   user name to connect as
     * @param password   password for the user name
     * @param namespace  Event Hub namespace
     * @param fqdnSuffix FQDN suffix for the Event Hub namespace url.
     * @param entityPath Name of the Event Hub
     */
    public EventHubBoltConfig(String userName, String password, String namespace, String fqdnSuffix,
                              String entityPath) {
        this(userName, password, namespace, fqdnSuffix, entityPath, false, null);
    }

    /**
     * Constructs an instance with specified credentials, and partition mode
     *
     * @param userName      user name to connect as
     * @param password      password for the user name
     * @param namespace     Event Hub namespace
     * @param fqdnSuffix    FQDN suffix for the Event Hub namespace url.
     * @param entityPath    Name of the Event Hub
     * @param useTaskIndexAsPartitionId if true will write to specific partition, uses TaskIndex from TopologyContext value to create Sender to Event Hub Partition
     */
    public EventHubBoltConfig(String userName, String password, String namespace, String fqdnSuffix, String entityPath,
                              boolean useTaskIndexAsPartitionId) {
        this(userName, password, namespace, fqdnSuffix, entityPath, useTaskIndexAsPartitionId, null);
    }

    /**
     * Constructs an instance with specified credentials, partition mode, and data
     * formatter
     *
     * @param userName      user name to connect as
     * @param password      password for the user name
     * @param namespace     Event Hub namespace
     * @param fqdnSuffix    FQDN suffix for the Event Hub namespace url.
     * @param entityPath    Name of the Event Hub
     * @param useTaskIndexAsPartitionId if true will write to specific partition, uses TaskIndex from TopologyContext value to create Sender to Event Hub Partition
     * @param dataFormat    data formatter for serializing event data
     * @see IEventDataFormat
     */
    public EventHubBoltConfig(String userName, String password, String namespace, String fqdnSuffix, String entityPath,
                              boolean useTaskIndexAsPartitionId, IEventDataFormat dataFormat) {
        URI uri = null;
        try {
            uri = new URI(String.format("amqps://%s.%s", namespace, fqdnSuffix));
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to construct EventHub connection URI.", e);
        }
        this.connectionString = new ConnectionStringBuilder()
                .setEndpoint(uri)
                .setSasKeyName(userName)
                .setSasKey(password)
                .setEventHubName(entityPath)
                .toString();
        this.useTaskIndexAsPartitionId = useTaskIndexAsPartitionId;
        this.dataFormat = dataFormat;
        if (this.dataFormat == null) {
            this.dataFormat = new DefaultEventDataFormat();
        }
    }

    /**
     * Event Hub Connection string
     *
     * @return connection string
     */
    public String getConnectionString() {
        return connectionString;
    }

    /**
     * @return returns partition mode configuration.
     * <p>
     * This is a legacy setting that will soon be deprecated. Please use the
     * {@link EventHubBoltConfig#getUseTaskIndexAsPartitionId()} instead.
     * </p>
     *
     * @deprecated
     */
    public boolean getPartitionMode() {
        return getUseTaskIndexAsPartitionId();
    }

    /**
     * @return returns the value of the flag which specifies whether to write to a specific Event Hub partition
     */
    public boolean getUseTaskIndexAsPartitionId() {
        return useTaskIndexAsPartitionId;
    }

    /**
     * Data formatter for event data
     *
     * @return Instance of {@link IEventDataFormat}
     */
    public IEventDataFormat getEventDataFormat() {
        return dataFormat;
    }
}
