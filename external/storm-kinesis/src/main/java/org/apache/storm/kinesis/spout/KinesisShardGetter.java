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

import java.util.concurrent.Callable;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.apache.storm.kinesis.spout.exceptions.InvalidSeekPositionException;
import org.apache.storm.kinesis.spout.exceptions.KinesisSpoutException;
import org.apache.storm.kinesis.spout.utils.InfiniteConstantBackoffRetry;
import com.google.common.collect.ImmutableList;

/**
 * Fetches data from a Kinesis shard.
 */
class KinesisShardGetter implements IShardGetter {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardGetter.class);

    private static final long BACKOFF_MILLIS = 500L;

    private final String streamName;
    private final String shardId;
    private final AmazonKinesisClient kinesisClient;

    private String shardIterator;
    private ShardPosition positionInShard;

    /**
     * @param streamName Name of the Kinesis stream
     * @param shardId Fetch data from this shard
     * @param kinesisClient Kinesis client to use when making requests.
     */
    KinesisShardGetter(final String streamName, final String shardId, final AmazonKinesisClient kinesisClient) {
        this.streamName = streamName;
        this.shardId = shardId;
        this.kinesisClient = kinesisClient;
        this.shardIterator = "";
        this.positionInShard = ShardPosition.end();
    }

    @Override
    public Records getNext(int maxNumberOfRecords)
        throws AmazonClientException, ResourceNotFoundException, InvalidArgumentException {
        if (shardIterator == null) {
            LOG.debug(this + " Null shardIterator for " + shardId + ". This can happen if shard is closed.");
            return Records.empty(true);
        }

        final ImmutableList.Builder<Record> records = new ImmutableList.Builder<>();
        
        try {
            final GetRecordsRequest request = new GetRecordsRequest();
            request.setShardIterator(shardIterator);
            request.setLimit(maxNumberOfRecords);
            final GetRecordsResult result = safeGetRecords(request);

            for (Record rec : result.getRecords()) {
                records.add(rec);
                positionInShard = ShardPosition.afterSequenceNumber(rec.getSequenceNumber());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(this + " fetched " + result.getRecords().size() + " records from Kinesis (requested "
                        + maxNumberOfRecords + ").");
            }

            shardIterator = result.getNextShardIterator();            
        } catch (AmazonClientException e) {
            // We'll treat this equivalent to fetching 0 records - the spout drives the retry as part of nextTuple()
            // We don't sleep here - we can continue processing ack/fail on the spout thread.
            LOG.error(this + "Caught exception when fetching records for " + shardId, e);
        }

        return new Records(records.build(), shardIterator == null);
    }

    @Override
    public void seek(ShardPosition position)
        throws AmazonClientException, ResourceNotFoundException, InvalidSeekPositionException {
        LOG.info("Seeking to " + position);

        ShardIteratorType iteratorType;
        String seqNum = null;
        switch (position.getPosition()) {
            case TRIM_HORIZON:
                iteratorType = ShardIteratorType.TRIM_HORIZON;
                break;
            case LATEST:
                iteratorType = ShardIteratorType.LATEST;
                break;
            case AT_SEQUENCE_NUMBER:
                iteratorType = ShardIteratorType.AT_SEQUENCE_NUMBER;
                seqNum = position.getSequenceNum();
                break;
            case AFTER_SEQUENCE_NUMBER:
                iteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER;
                seqNum = position.getSequenceNum();
                break;
            default:
                LOG.error("Invalid seek position " + position);
                throw new InvalidSeekPositionException(position);
        }

        try {
            shardIterator = seek(iteratorType, seqNum);
        } catch (InvalidArgumentException e) {
            LOG.error("Error occured while seeking, cannot seek to " + position + ".", e);
            throw new InvalidSeekPositionException(position);
        } catch (Exception e) {
            LOG.error("Irrecoverable exception, rethrowing.", e);
            throw e;
        }

        positionInShard = position;
    }

    @Override
    public String getAssociatedShard() {
        return shardId;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("shardId", shardId).toString();
    }

    private String seek(final ShardIteratorType iteratorType, final String seqNum)
        throws AmazonClientException, ResourceNotFoundException, InvalidArgumentException {
        final GetShardIteratorRequest request = new GetShardIteratorRequest();

        request.setStreamName(streamName);
        request.setShardId(shardId);
        request.setShardIteratorType(iteratorType);

        // SeqNum is only set on {AT, AFTER}_SEQUENCE_NUMBER, so this is safe.
        if (seqNum != null) {
            request.setStartingSequenceNumber(seqNum);
        }

        return new InfiniteConstantBackoffRetry<String>(BACKOFF_MILLIS,
                AmazonClientException.class,
                new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        GetShardIteratorResult result = kinesisClient.getShardIterator(request);
                        return result.getShardIterator();
                    }
                }).call();
    }

    private GetRecordsResult safeGetRecords(final GetRecordsRequest request)
        throws AmazonClientException, ResourceNotFoundException, InvalidArgumentException {
        while (true) {
            try {
                return kinesisClient.getRecords(request);
            } catch (ExpiredIteratorException e) {
                LOG.info("Expired shard iterator, seeking to last known position.");
                try {
                    seek(positionInShard);
                } catch (InvalidSeekPositionException e1) {
                    LOG.error("Could not seek to last known position after iterator expired.");
                    throw new KinesisSpoutException(e1);
                }
                request.setShardIterator(shardIterator);
            }
        }
    }
}
