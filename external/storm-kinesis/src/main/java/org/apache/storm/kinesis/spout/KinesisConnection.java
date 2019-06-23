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

import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KinesisConnection {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisRecordsManager.class);
    private final KinesisConnectionInfo kinesisConnectionInfo;
    private AmazonKinesisClient kinesisClient;

    KinesisConnection(KinesisConnectionInfo kinesisConnectionInfo) {
        this.kinesisConnectionInfo = kinesisConnectionInfo;
    }

    void initialize() {
        kinesisClient = new AmazonKinesisClient(kinesisConnectionInfo.getCredentialsProvider(),
                kinesisConnectionInfo.getClientConfiguration());
        kinesisClient.setRegion(Region.getRegion(kinesisConnectionInfo.getRegion()));
    }

    List<Shard> getShardsForStream(String stream) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(stream);
        List<Shard> shards = new ArrayList<>();
        String exclusiveStartShardId = null;
        do {
            describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
            DescribeStreamResult describeStreamResult = kinesisClient.describeStream(describeStreamRequest);
            shards.addAll(describeStreamResult.getStreamDescription().getShards());
            if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
                exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
            } else {
                exclusiveStartShardId = null;
            }
        } while (exclusiveStartShardId != null);
        LOG.info("Number of shards for stream " + stream + " are " + shards.size());
        return shards;
    }

    String getShardIterator(String stream,
            String shardId,
            ShardIteratorType shardIteratorType,
            String sequenceNumber,
            Date timestamp) {
        String shardIterator = "";
        try {
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            getShardIteratorRequest.setStreamName(stream);
            getShardIteratorRequest.setShardId(shardId);
            getShardIteratorRequest.setShardIteratorType(shardIteratorType);
            if (shardIteratorType.equals(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    || shardIteratorType.equals(ShardIteratorType.AT_SEQUENCE_NUMBER)) {
                getShardIteratorRequest.setStartingSequenceNumber(sequenceNumber);
            } else if (shardIteratorType.equals(ShardIteratorType.AT_TIMESTAMP)) {
                getShardIteratorRequest.setTimestamp(timestamp);
            }
            GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
            if (getShardIteratorResult != null) {
                shardIterator = getShardIteratorResult.getShardIterator();
            }
        } catch (Exception e) {
            LOG.warn("Exception occured while getting shardIterator for shard " + shardId
                    + " shardIteratorType " + shardIteratorType
                    + " sequence number " + sequenceNumber
                    + " timestamp " + timestamp,
                    e);
        }
        LOG.warn("Returning shardIterator " + shardIterator
                + " for shardId " + shardId
                + " shardIteratorType " + shardIteratorType
                + " sequenceNumber " + sequenceNumber
                + " timestamp" + timestamp);
        return shardIterator;
    }

    GetRecordsResult fetchRecords(String shardIterator) {
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(shardIterator);
        getRecordsRequest.setLimit(kinesisConnectionInfo.getRecordsLimit());
        GetRecordsResult getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
        return getRecordsResult;
    }

    void shutdown() {
        kinesisClient.shutdown();
    }

}
