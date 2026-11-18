/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cluster;

/**
 * Connection state of a ZooKeeper client, as delivered to a {@link ClusterStateListener}.
 */
public enum ConnectionState {
    /** The first successful connection. */
    CONNECTED,
    /** The connection was re-established after being suspended or lost. */
    RECONNECTED,
    /** The session is considered lost (expired or the connection timed out). */
    LOST,
    /** The connection was interrupted (ZooKeeper Disconnected); the session may still be alive. */
    SUSPENDED,
    /** Connected in read-only mode (ZooKeeper ConnectedReadOnly). */
    READ_ONLY
}
