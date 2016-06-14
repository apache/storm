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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import org.apache.storm.kinesis.spout.utils.InfiniteConstantBackoffRetry;
import org.apache.storm.kinesis.spout.utils.ShardIdComparator;
import com.google.common.collect.ImmutableSortedMap;

/**
 * Helper class to fetch the shard list from Kinesis, create Kinesis client objects, etc.
 */
class KinesisHelper implements IShardListGetter {
    private static final long serialVersionUID = 4175914620613267032L;
    private static final Logger LOG = LoggerFactory.getLogger(KinesisHelper.class);
    private static final ShardIdComparator SHARD_ID_COMPARATOR = new ShardIdComparator();
    private static final Integer DESCRIBE_STREAM_LIMIT = 1000;
    private static final String KINESIS_STORM_SPOUT_USER_AGENT = "kinesis-storm-spout-java-1.1.1";
    private static final long BACKOFF_MILLIS = 1000L;

    private final byte[] serializedKinesisCredsProvider;
    private final byte[] serializedkinesisClientConfig;
    private final byte[] serializedRegion;
    private final String streamName;

    private transient AWSCredentialsProvider kinesisCredsProvider;
    private transient ClientConfiguration kinesisClientConfig;
    private transient AmazonKinesisClient kinesisClient;
    private transient Region region;

    /**
     * @param streamName Kinesis stream name to interact with.
     * @param kinesisCredsProvider Credentials for authentication with Kinesis.
     * @param kinesisClientConfig Configuration for the Kinesis client.
     */
    KinesisHelper(final String streamName,
            final AWSCredentialsProvider kinesisCredsProvider,
            final ClientConfiguration kinesisClientConfig,
            final Region region) {
        this.streamName = streamName;
        this.serializedKinesisCredsProvider = SerializationHelper.kryoSerializeObject(kinesisCredsProvider);
        this.serializedkinesisClientConfig = SerializationHelper.kryoSerializeObject(kinesisClientConfig);
        this.serializedRegion = SerializationHelper.kryoSerializeObject(region);

        this.kinesisCredsProvider = null;
        this.kinesisClientConfig = null;
        this.region = null;
        this.kinesisClient = null;
    }

    @Override
    public ImmutableSortedMap<String, ShardInfo> getShardList() {
        Map<String, ShardInfo> spoutShards = new HashMap<>();

        DescribeStreamRequest input = new DescribeStreamRequest();
        DescribeStreamResult out;

        input.setStreamName(streamName);
        input.setLimit(DESCRIBE_STREAM_LIMIT);
        out = getDescribeStreamResult(input);

        while (true) {
            String lastShard = addTruncatedShardList(spoutShards, out.getStreamDescription().getShards());

            // If we have finished processing all the shards, we can stop looping
            if (!out.getStreamDescription().isHasMoreShards()) {
                break;
            }

            LOG.debug("There are more shards in the stream, continue paginated calls.");
            input.setExclusiveStartShardId(lastShard);
            out = getDescribeStreamResult(input);
        }

        return ImmutableSortedMap.copyOf(spoutShards, SHARD_ID_COMPARATOR);
    }

    private DescribeStreamResult getDescribeStreamResult(final DescribeStreamRequest request) {
        return new InfiniteConstantBackoffRetry<DescribeStreamResult>(BACKOFF_MILLIS, AmazonClientException.class,
                new Callable<DescribeStreamResult>() {
            public DescribeStreamResult call() throws Exception {
                DescribeStreamResult result = getSharedkinesisClient().describeStream(request);
                return result;
            }
        }).call();
    }

    /**
     * @return new instance of AmazonKinesisClient, with parameters supplied by whatever was passed
     *         to the KinesisHelper constructor.
     */
    private AmazonKinesisClient makeNewKinesisClient() {
        AmazonKinesisClient client = new AmazonKinesisClient(getKinesisCredsProvider(), getClientConfiguration());
        LOG.info("Using " + getRegion().getName() + " region");
        client.setRegion(getRegion());
        return client;
    }

    AmazonKinesisClient getSharedkinesisClient() {
        if (kinesisClient == null) {
            kinesisClient = makeNewKinesisClient();
        }
        return kinesisClient;
    }

    private AWSCredentialsProvider getKinesisCredsProvider() {
        if (kinesisCredsProvider == null) {
            kinesisCredsProvider =
                    (AWSCredentialsProvider) SerializationHelper.kryoDeserializeObject(serializedKinesisCredsProvider);
        }
        return kinesisCredsProvider;
    }

    private ClientConfiguration getClientConfiguration() {
        if (kinesisClientConfig == null) {
            kinesisClientConfig =
                    (ClientConfiguration) SerializationHelper.kryoDeserializeObject(serializedkinesisClientConfig);
        }
        String userAgent = kinesisClientConfig.getUserAgent();
        if (!userAgent.contains(KINESIS_STORM_SPOUT_USER_AGENT)) {
            userAgent += ", " + KINESIS_STORM_SPOUT_USER_AGENT;
            kinesisClientConfig.setUserAgent(userAgent);
        }
        return kinesisClientConfig;
    }

    private Region getRegion() {
        if (region == null) {
            region = (Region) SerializationHelper.kryoDeserializeObject(serializedRegion);
        }
        return region;
    }

    private String addTruncatedShardList(final Map<String, ShardInfo> spoutShards, final List<Shard> streamShards) {
        String currShard = "";

        for (Shard s : streamShards) {
            currShard = s.getShardId();
            spoutShards.put(s.getShardId(), new ShardInfo(s.getShardId()));

            if (s.getParentShardId() != null && s.getAdjacentParentShardId() != null) {
                // It's a merge. Set both parents of the merge to merge into this shard.
                ShardInfo parentShardInfo = spoutShards.get(s.getParentShardId());
                ShardInfo adjacentParentShardInfo = spoutShards.get(s.getAdjacentParentShardId());
                if ((parentShardInfo != null) && (adjacentParentShardInfo != null)) {
                    parentShardInfo.setMergesInto(s.getShardId());
                    adjacentParentShardInfo.setMergesInto(s.getShardId());
                }
            } else if (s.getParentShardId() != null) {
                // It's a split. Add the current shard to the split list of its parent.
                ShardInfo parentShardInfo = spoutShards.get(s.getParentShardId());
                if (parentShardInfo != null) {
                    parentShardInfo.addSplitsInto(s.getShardId());
                }
            }
        }

        return currShard;
    }
}
