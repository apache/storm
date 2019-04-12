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

public class FieldConstants {
    public static final String PartitionKey = "partitionKey";
    public static final String Offset = "offset";
    public static final String MESSAGE_FIELD = "message";
    public static final String META_DATA_FIELD = "metadata";
    public static final String DefaultStartingOffset = "-1";
    public static final int DEFAULT_EVENTHUB_RECEIVE_TIMEOUT_MS = 5000;
    public static final String EH_SERVICE_FQDN_SUFFIX = "servicebus.windows.net";
    public static final int DEFAULT_RECEIVE_MAX_CAP = 1;
    public static final String SYSTEM_PROPERTIES_FIELD = "systemProperties";
    public static final String PARTITION_ID_KEY = "partitionId";
    public static final int DEFAULT_PREFETCH_COUNT = 999;
    public static final int DEFAULT_MAX_PENDING_PER_PARTITION = 1024;
    public static final long DEFAULT_SEQUENCE_NUMBER = 0L;  
}
