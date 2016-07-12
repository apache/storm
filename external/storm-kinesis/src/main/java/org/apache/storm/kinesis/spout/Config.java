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

import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;

public class Config implements Serializable {
    // kinesis stream name to read from
    private final String streamName;
    // shard iterator type based on kinesis api - beginning of time, latest, at timestamp are only supported
    private final ShardIteratorType shardIteratorType;
    // implementation for converting a Kinesis record to a storm tuple
    private final RecordToTupleMapper recordToTupleMapper;
    // timestamp to be used for shardIteratorType AT_TIMESTAMP - can be null
    private final Date timestamp;
    // implementation for handling the failed messages retry logic
    private final FailedMessageRetryHandler failedMessageRetryHandler;
    // object capturing all zk related information for storing committed sequence numbers
    private final ZkInfo zkInfo;
    // object representing information on paramaters to use while connecting to kinesis using kinesis client
    private final KinesisConnectionInfo kinesisConnectionInfo;
    // this number represents the number of messages that are still not committed to zk. it will prevent the spout from emitting further.
    // for e.g. if 1 failed and 2,3,4,5..... all have been acked by storm, they still cant be committed to zk because 1 is still in failed set. As a result
    // the acked queue can infinitely grow without any of them being committed to zk. topology max pending does not help since from storm's view they are acked
    private final Long maxUncommittedRecords;

    public Config (String streamName, ShardIteratorType shardIteratorType, RecordToTupleMapper recordToTupleMapper, Date timestamp, FailedMessageRetryHandler
            failedMessageRetryHandler, ZkInfo zkInfo, KinesisConnectionInfo kinesisConnectionInfo, Long maxUncommittedRecords) {
        this.streamName = streamName;
        this.shardIteratorType = shardIteratorType;
        this.recordToTupleMapper = recordToTupleMapper;
        this.timestamp = timestamp;
        this.failedMessageRetryHandler = failedMessageRetryHandler;
        this.zkInfo = zkInfo;
        this.kinesisConnectionInfo = kinesisConnectionInfo;
        this.maxUncommittedRecords = maxUncommittedRecords;
        validate();
    }

    private void validate () {
        if (streamName == null || streamName.length() < 1) {
            throw new IllegalArgumentException("streamName is required and cannot be of length 0.");
        }
        if (shardIteratorType == null || shardIteratorType.equals(ShardIteratorType.AFTER_SEQUENCE_NUMBER) || shardIteratorType.equals(ShardIteratorType
                .AT_SEQUENCE_NUMBER)) {
            throw new IllegalArgumentException("shardIteratorType has to be one of the " + ShardIteratorType.AT_TIMESTAMP + "," + ShardIteratorType.LATEST +
                    "," + ShardIteratorType.TRIM_HORIZON);
        }
        if (shardIteratorType.equals(ShardIteratorType.AT_TIMESTAMP) && timestamp == null) {
            throw new IllegalArgumentException("timestamp must be provided if shardIteratorType is " + ShardIteratorType.AT_TIMESTAMP);
        }
        if (recordToTupleMapper == null) {
            throw new IllegalArgumentException("recordToTupleMapper cannot be null");
        }
        if (failedMessageRetryHandler == null) {
            throw new IllegalArgumentException("failedMessageRetryHandler cannot be null");
        }
        if (zkInfo == null) {
            throw new IllegalArgumentException("zkInfo cannot be null");
        }
        if (kinesisConnectionInfo == null) {
            throw new IllegalArgumentException("kinesisConnectionInfo cannot be null");
        }
        if (maxUncommittedRecords == null || maxUncommittedRecords < 1) {
            throw new IllegalArgumentException("maxUncommittedRecords has to be a positive integer");
        }
    }

    public String getStreamName() {
        return streamName;
    }

    public ShardIteratorType getShardIteratorType() {
        return shardIteratorType;
    }

    public RecordToTupleMapper getRecordToTupleMapper() {
        return recordToTupleMapper;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public FailedMessageRetryHandler getFailedMessageRetryHandler () {
        return failedMessageRetryHandler;
    }

    public ZkInfo getZkInfo () {
        return zkInfo;
    }

    public KinesisConnectionInfo getKinesisConnectionInfo () {
        return kinesisConnectionInfo;
    }

    public Long getMaxUncommittedRecords () {
        return maxUncommittedRecords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Config config = (Config) o;

        if (streamName != null ? !streamName.equals(config.streamName) : config.streamName != null) return false;
        if (shardIteratorType != config.shardIteratorType) return false;
        if (recordToTupleMapper != null ? !recordToTupleMapper.equals(config.recordToTupleMapper) : config.recordToTupleMapper != null) return false;
        if (timestamp != null ? !timestamp.equals(config.timestamp) : config.timestamp != null) return false;
        if (zkInfo != null ? !zkInfo.equals(config.zkInfo) : config.zkInfo != null) return false;
        if (kinesisConnectionInfo != null ? !kinesisConnectionInfo.equals(config.kinesisConnectionInfo) : config.kinesisConnectionInfo != null) return false;
        if (maxUncommittedRecords != null ? !maxUncommittedRecords.equals(config.maxUncommittedRecords) : config.maxUncommittedRecords != null) return false;
        return !(failedMessageRetryHandler != null ? !failedMessageRetryHandler.equals(config.failedMessageRetryHandler) : config.failedMessageRetryHandler
                != null);
    }

    @Override
    public int hashCode() {
        int result = streamName != null ? streamName.hashCode() : 0;
        result = 31 * result + (shardIteratorType != null ? shardIteratorType.hashCode() : 0);
        result = 31 * result + (recordToTupleMapper != null ? recordToTupleMapper.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (zkInfo != null ? zkInfo.hashCode() : 0);
        result = 31 * result + (kinesisConnectionInfo != null ? kinesisConnectionInfo.hashCode() : 0);
        result = 31 * result + (failedMessageRetryHandler != null ? failedMessageRetryHandler.hashCode() : 0);
        result = 31 * result + (maxUncommittedRecords != null ? maxUncommittedRecords.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Config{" +
                "streamName='" + streamName + '\'' +
                ", shardIteratorType=" + shardIteratorType +
                ", recordToTupleMapper=" + recordToTupleMapper +
                ", timestamp=" + timestamp +
                ", zkInfo=" + zkInfo +
                ", kinesisConnectionInfo=" + kinesisConnectionInfo +
                ", failedMessageRetryHandler =" + failedMessageRetryHandler +
                ", maxUncommittedRecords=" + maxUncommittedRecords +
                '}';
    }
}
