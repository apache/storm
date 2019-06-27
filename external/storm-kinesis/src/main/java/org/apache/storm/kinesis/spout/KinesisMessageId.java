/**
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
 */

package org.apache.storm.kinesis.spout;

public class KinesisMessageId {
    private final String streamName;
    private final String shardId;
    private final String sequenceNumber;

    KinesisMessageId(String streamName, String shardId, String sequenceNumber) {
        this.streamName = streamName;
        this.shardId = shardId;
        this.sequenceNumber = sequenceNumber;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getShardId() {
        return shardId;
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String toString() {
        return "KinesisMessageId{"
                + "streamName='" + streamName + '\''
                + ", shardId='" + shardId + '\''
                + ", sequenceNumber='" + sequenceNumber + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KinesisMessageId that = (KinesisMessageId) o;

        if (streamName != null ? !streamName.equals(that.streamName) : that.streamName != null) {
            return false;
        }
        if (shardId != null ? !shardId.equals(that.shardId) : that.shardId != null) {
            return false;
        }
        return !(sequenceNumber != null ? !sequenceNumber.equals(that.sequenceNumber) : that.sequenceNumber != null);

    }

    @Override
    public int hashCode() {
        int result = streamName != null ? streamName.hashCode() : 0;
        result = 31 * result + (shardId != null ? shardId.hashCode() : 0);
        result = 31 * result + (sequenceNumber != null ? sequenceNumber.hashCode() : 0);
        return result;
    }
}
