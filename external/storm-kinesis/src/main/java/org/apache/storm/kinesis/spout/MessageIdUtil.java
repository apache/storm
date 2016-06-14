/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kinesis.spout;

/**
 * Helper class to convert (shardId, sequenceNumber) <-> messageId.
 */
class MessageIdUtil {

    private MessageIdUtil() {
    }

    /**
     * @param messageId MessageId of the tuple.
     * @return shardId of the record (tuple).
     */
    static String shardIdOfMessageId(String messageId) {
        return messageId.split(":")[0];
    }

    /**
     * @param messageId MessageId.
     * @return sequence number for the record (tuple)
     */
    static String sequenceNumberOfMessageId(String messageId) {
        return messageId.split(":")[1];
    }

    /**
     * Used to construct a messageId for a tuple corresponding to a Kinesis record.
     * @param shardId Shard from which the record was fetched.
     * @param sequenceNumber Sequence number of the record.
     * @return messageId
     */
    static Object constructMessageId(String shardId, String sequenceNumber) {
        String messageId = shardId + ":" + sequenceNumber;
        return messageId;
    }

}
