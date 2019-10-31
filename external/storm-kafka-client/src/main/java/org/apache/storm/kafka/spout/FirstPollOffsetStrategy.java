/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout;

/**
 * Defines how the spout seeks the offset to be used in the first poll to Kafka upon topology deployment. By default this parameter is set
 * to UNCOMMITTED_EARLIEST.
 */
public enum FirstPollOffsetStrategy {
    /**
     * The kafka spout polls records starting in the first offset of the partition, regardless of previous commits. This setting only takes
     * effect on topology deployment
     */
    EARLIEST,
    /**
     * The kafka spout polls records starting at the end of the partition, regardless of previous commits. This setting only takes effect on
     * topology deployment
     */
    LATEST,
    /**
     * The kafka spout polls records starting at the earliest offset whose timestamp is greater than or equal to the given startTimestamp.
     * This setting only takes effect on topology deployment. This option is currently available only for the Trident Spout
     */
    TIMESTAMP,
    /**
     * The kafka spout polls records from the last committed offset, if any. If no offset has been committed it behaves as EARLIEST
     */
    UNCOMMITTED_EARLIEST,
    /**
     * The kafka spout polls records from the last committed offset, if any. If no offset has been committed it behaves as LATEST
     */
    UNCOMMITTED_LATEST,
    /**
     * The kafka spout polls records from the last committed offset, if any. If no offset has been committed it behaves as TIMESTAMP.
     * This option is currently available only for the Trident Spout
     */
    UNCOMMITTED_TIMESTAMP;
}
