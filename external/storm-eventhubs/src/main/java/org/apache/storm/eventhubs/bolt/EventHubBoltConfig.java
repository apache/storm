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

import com.microsoft.azure.servicebus.ConnectionStringBuilder;

import org.apache.storm.eventhubs.core.FieldConstants;
import org.apache.storm.eventhubs.format.DefaultEventDataFormat;
import org.apache.storm.eventhubs.format.IEventDataFormat;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/*
 * EventHubs bolt configurations
 *
 * Partition mode:
 * With partitionMode=true you need to create the same number of tasks as the number of 
 * EventHubs partitions, and each bolt task will only send data to one partition.
 * The partition ID is the task ID of the bolt.
 * 
 * Event format:
 * The formatter to convert tuple to bytes for EventHubs.
 * if null, the default format is common delimited tuple fields.
 */
public class EventHubBoltConfig implements Serializable {
    private static final long serialVersionUID = 1L;
  
    private String connectionString;
    protected boolean partitionMode;
    protected IEventDataFormat dataFormat;
  
    public EventHubBoltConfig(String connectionString, String entityPath) {
        this(connectionString, false, null);
    }
  
    public EventHubBoltConfig(String connectionString, String entityPath, boolean partitionMode) {
        this(connectionString, partitionMode, null);
    }
  
    public EventHubBoltConfig(String userName, String password, String namespace,
            String entityPath, boolean partitionMode) {
        this(userName, password, namespace, FieldConstants.EH_SERVICE_FQDN_SUFFIX,
            entityPath, partitionMode);
    }
  
    public EventHubBoltConfig(String connectionString, boolean partitionMode, IEventDataFormat dataFormat) {
        this.connectionString = connectionString;
        this.partitionMode = partitionMode;
        this.dataFormat = dataFormat;
        if (this.dataFormat == null) {
            this.dataFormat = new DefaultEventDataFormat();
        }
    }
  
    public EventHubBoltConfig(String userName, String password, String namespace,
            String fqdnSuffix, String entityPath) {
        this(userName, password, namespace, fqdnSuffix, entityPath, false, null);
    }
  
    public EventHubBoltConfig(String userName, String password, String namespace,
            String fqdnSuffix, String entityPath, boolean partitionMode) {
        this(userName, password, namespace, fqdnSuffix, entityPath, partitionMode, null);
    }
  
    public EventHubBoltConfig(String userName, String password, String namespace,
            String fqdnSuffix, String entityPath, boolean partitionMode,
            IEventDataFormat dataFormat) {
        URI uri = null;
        try {
            uri = new URI(String.format("amqps://%s.%s", new Object[] { namespace, fqdnSuffix }));
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to construct EventHub connection URI.", e);
        }	  
        this.connectionString = new ConnectionStringBuilder(uri, userName, password).toString();
        this.partitionMode = partitionMode;
        this.dataFormat = dataFormat;
        if (this.dataFormat == null) {
            this.dataFormat = new DefaultEventDataFormat();
        }
    }
  
    public String getConnectionString() {
        return connectionString;
    }
  
    public boolean getPartitionMode() {
        return partitionMode;
    }
  
    public IEventDataFormat getEventDataFormat() {
        return dataFormat;
    }
}
