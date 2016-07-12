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
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

class KinesisRecordsManager {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisRecordsManager.class);
    // zk interaction object
    private transient CuratorFramework curatorFramework;
    // Kinesis Spout Config object
    private transient final Config config;
    // Queue of records per shard fetched from kinesis and are waiting to be emitted
    private transient Map<String, LinkedList<Record>> toEmitPerShard = new HashMap<>();
    // Map of records  that were fetched from kinesis as a result of failure and are waiting to be emitted
    private transient Map<KinesisMessageId, Record> failedandFetchedRecords = new HashMap<>();
    // Sequence numbers per shard that have been emitted. LinkedHashSet as we need to remove on ack or fail. At the same time order is needed to figure out the
    // sequence number to commit. Logic explained in commit
    private transient Map<String, TreeSet<BigInteger>> emittedPerShard = new HashMap<>();
    // sorted acked sequence numbers - needed to figure out what sequence number can be committed
    private transient Map<String, TreeSet<BigInteger>> ackedPerShard = new HashMap<>();
    // sorted failed sequence numbers - needed to figure out what sequence number can be committed
    private transient Map<String, TreeSet<BigInteger>> failedPerShard = new HashMap<>();
    // shard iterator corresponding to position in shard for new messages
    private transient Map<String, String> shardIteratorPerShard = new HashMap<>();
    // last fetched sequence number corresponding to position in shard
    private transient Map<String, String> fetchedSequenceNumberPerShard = new HashMap<>();
    // shard iterator corresponding to position in shard for failed messages
    private transient Map<KinesisMessageId, String> shardIteratorPerFailedMessage = new HashMap<>();
    // timestamp to decide when to commit to zk again
    private transient long lastCommitTime;
    // boolean to track deactivated state
    private transient boolean deactivated;
    private transient AmazonKinesisClient kinesisClient;

    KinesisRecordsManager (Config config) {
        this.config = config;
    }

    void initialize (int myTaskIndex, int totalTasks) {
        deactivated = false;
        lastCommitTime = System.currentTimeMillis();
        initializeKinesisClient();
        initializeCurator();
        List<Shard> shards = this.getShards();
        LOG.info("myTaskIndex is " + myTaskIndex);
        LOG.info("totalTasks is " + totalTasks);
        int i = myTaskIndex;
        while (i < shards.size()) {
            LOG.info("Shard id " + shards.get(i).getShardId() + " assigned to task " + myTaskIndex);
            toEmitPerShard.put(shards.get(i).getShardId(), new LinkedList<Record>());
            i += totalTasks;
        }
        initializeFetchedSequenceNumbers();
        refreshShardIteratorsForNewRecords();
    }

    void next (SpoutOutputCollector collector) {
        if (shouldCommit()) {
            commit();
        }
        KinesisMessageId failedMessageId = config.getFailedMessageRetryHandler().getNextFailedMessageToRetry();
        if (failedMessageId  != null) {
            // if the retry service returns a message that is not in failed set then ignore it. should never happen
            BigInteger failedSequenceNumber = new BigInteger(failedMessageId.getSequenceNumber());
            if (failedPerShard.containsKey(failedMessageId.getShardId()) && failedPerShard.get(failedMessageId.getShardId()).contains(failedSequenceNumber)) {
                if (!failedandFetchedRecords.containsKey(failedMessageId)) {
                    fetchFailedRecords(failedMessageId);
                }
                if (emitFailedRecord(collector, failedMessageId)) {
                    failedPerShard.get(failedMessageId.getShardId()).remove(failedSequenceNumber);
                    config.getFailedMessageRetryHandler().failedMessageEmitted(failedMessageId);
                    return;
                } else {
                    LOG.debug("failedMessageEmitted not called on retrier for " + failedMessageId + ". This can happen a few times but should not happen " +
                            "infinitely");
                }
            } else {
                LOG.debug("failedPerShard does not contain " + failedMessageId + ". This should never happen.");
            }
        }
        LOG.debug("No failed record to emit for now. Hence will try to emit new records");
        // if maximum uncommitted records count has reached, so dont emit any new records and return
        if (!(getUncommittedRecordsCount() < config.getMaxUncommittedRecords())) {
            LOG.debug("maximum uncommitted records count has reached. so not emitting any new records and returning");
            return;
        }
        // early return as no shard is assigned - probably because number of executors > number of shards
        if (toEmitPerShard.isEmpty()) {
            LOG.debug("No shard is assigned to this task. Hence not emitting any tuple.");
            return;
        }

        if (shouldFetchNewRecords()) {
            fetchNewRecords();
        }
        emitNewRecord(collector);
    }

    void ack (KinesisMessageId kinesisMessageId) {
        // for an acked message add it to acked set and remove it from emitted and failed
        String shardId = kinesisMessageId.getShardId();
        BigInteger sequenceNumber = new BigInteger(kinesisMessageId.getSequenceNumber());
        LOG.debug("Ack received for shardId: " + shardId + " sequenceNumber: " + sequenceNumber);
        if (!ackedPerShard.containsKey(shardId)) {
            ackedPerShard.put(shardId, new TreeSet<BigInteger>());
        }
        ackedPerShard.get(shardId).add(sequenceNumber);
        if (emittedPerShard.containsKey(shardId)) {
            TreeSet<BigInteger> emitted = emittedPerShard.get(shardId);
            emitted.remove(sequenceNumber);
        }
        if (failedPerShard.containsKey(shardId)) {
            failedPerShard.get(shardId).remove(sequenceNumber);
        }
        if (failedandFetchedRecords.containsKey(kinesisMessageId)) {
            config.getFailedMessageRetryHandler().acked(kinesisMessageId);
            failedandFetchedRecords.remove(kinesisMessageId);
        }
        // keep committing when topology is deactivated since ack and fail keep getting called on deactivated topology
        if (deactivated) {
            commit();
        }
    }

    void fail (KinesisMessageId kinesisMessageId) {
        String shardId = kinesisMessageId.getShardId();
        BigInteger sequenceNumber = new BigInteger(kinesisMessageId.getSequenceNumber());
        LOG.debug("Fail received for shardId: " + shardId + " sequenceNumber: " + sequenceNumber);
        // for a failed message add it to failed set if it will be retried, otherwise ack it; remove from emitted either way
        if (config.getFailedMessageRetryHandler().failed(kinesisMessageId)) {
            if (!failedPerShard.containsKey(shardId)) {
                failedPerShard.put(shardId, new TreeSet<BigInteger>());
            }
            failedPerShard.get(shardId).add(sequenceNumber);
            TreeSet<BigInteger> emitted = emittedPerShard.get(shardId);
            emitted.remove(sequenceNumber);
        } else {
            ack(kinesisMessageId);
        }
        // keep committing when topology is deactivated since ack and fail keep getting called on deactivated topology
        if (deactivated) {
            commit();
        }
    }

    void commit () {
        // Logic for deciding what sequence number to ack is find the highest sequence number from acked called X such that there is no sequence number Y in
        // emitted or failed that satisfies X > Y. For e.g. is acked is 1,3,5. Emitted is 2,4,6 then we can only commit 1 and not 3 because 2 is still pending
        for (String shardId: toEmitPerShard.keySet()) {
            if (ackedPerShard.containsKey(shardId)) {
                BigInteger commitSequenceNumberBound = null;
                if (failedPerShard.containsKey(shardId) && !failedPerShard.get(shardId).isEmpty()) {
                    commitSequenceNumberBound = failedPerShard.get(shardId).first();
                }
                if (emittedPerShard.containsKey(shardId) && !emittedPerShard.get(shardId).isEmpty()) {
                    BigInteger smallestEmittedSequenceNumber = emittedPerShard.get(shardId).first();
                    if (commitSequenceNumberBound == null || (commitSequenceNumberBound.compareTo(smallestEmittedSequenceNumber) == 1)) {
                        commitSequenceNumberBound = smallestEmittedSequenceNumber;
                    }
                }
                Iterator<BigInteger> ackedSequenceNumbers = ackedPerShard.get(shardId).iterator();
                BigInteger ackedSequenceNumberToCommit = null;
                while (ackedSequenceNumbers.hasNext()) {
                    BigInteger ackedSequenceNumber = ackedSequenceNumbers.next();
                    if (commitSequenceNumberBound == null || (commitSequenceNumberBound.compareTo(ackedSequenceNumber) == 1)) {
                        ackedSequenceNumberToCommit = ackedSequenceNumber;
                        ackedSequenceNumbers.remove();
                    } else {
                        break;
                    }
                }
                if (ackedSequenceNumberToCommit != null) {
                    Map<Object, Object> state = new HashMap<>();
                    state.put("committedSequenceNumber", ackedSequenceNumberToCommit.toString());
                    LOG.debug("Committing sequence number " + ackedSequenceNumberToCommit.toString() + " for shardId " + shardId);
                    String path = getZkPath(shardId);
                    commitState(path, state);
                }
            }
        }
        lastCommitTime = System.currentTimeMillis();
    }

    void activate () {
        LOG.info("Activate called");
        deactivated = false;
        initializeKinesisClient();
    }

    void deactivate () {
        LOG.info("Deactivate called");
        deactivated = true;
        commit();
        shutdownKinesisClient();
    }

    void close () {
        commit();
        shutdownKinesisClient();
        shutdownCurator();
    }

    private String getZkPath (String shardId) {
        String path = "";
        if (!config.getZkInfo().getZkNode().startsWith("/")) {
            path += "/";
        }
        path += config.getZkInfo().getZkNode();
        if (!config.getZkInfo().getZkNode().endsWith("/")) {
            path += "/";
        }
        path += (config.getStreamName() + "/" + shardId);
        return path;
    }

    private void commitState (String path, Map<Object, Object> state) {
        byte[] bytes = JSONValue.toJSONString(state).getBytes(Charset.forName("UTF-8"));
        try {
            if (curatorFramework.checkExists().forPath(path) == null) {
                curatorFramework.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, bytes);
            } else {
                curatorFramework.setData().forPath(path, bytes);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Object, Object> readState (String path) {
        try {
            Map<Object, Object> state = null;
            byte[] b = null;
            if (curatorFramework.checkExists().forPath(path) != null) {
                b = curatorFramework.getData().forPath(path);
            }
            if (b != null) {
                state = (Map<Object, Object>) JSONValue.parse(new String(b, "UTF-8"));
            }
            return state;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // fetch records from kinesis starting at sequence number for message passed as argument. Any other messages fetched and are in the failed queue will also
    // be kept in memory to avoid going to kinesis again for retry
    private void fetchFailedRecords (KinesisMessageId kinesisMessageId) {
        // if shard iterator not present for this message, get it
        if (!shardIteratorPerFailedMessage.containsKey(kinesisMessageId)) {
            refreshShardIteratorForFailedRecord(kinesisMessageId);
        }
        String shardIterator = shardIteratorPerFailedMessage.get(kinesisMessageId);
        LOG.debug("Fetching failed records for shard id :" + kinesisMessageId.getShardId() + " at sequence number " + kinesisMessageId.getSequenceNumber() +
                " using shardIterator " + shardIterator);
        try {
            GetRecordsResult getRecordsResult = fetchRecords(shardIterator);
            if (getRecordsResult != null) {
                List<Record> records = getRecordsResult.getRecords();
                LOG.debug("Records size from fetchFailedRecords is " + records.size());
                // update the shard iterator to next one in case this fetch does not give the message.
                shardIteratorPerFailedMessage.put(kinesisMessageId, getRecordsResult.getNextShardIterator());
                if (records.size() == 0) {
                    LOG.debug("No records returned from kinesis. Hence sleeping for 1 second");
                    Thread.sleep(1000);
                } else {
                    // add all fetched records to the set of failed records if they are present in failed set
                    for (Record record: records) {
                        KinesisMessageId current = new KinesisMessageId(kinesisMessageId.getStreamName(), kinesisMessageId.getShardId(), record.getSequenceNumber());
                        if (failedPerShard.get(kinesisMessageId.getShardId()).contains(new BigInteger(current.getSequenceNumber()))) {
                            failedandFetchedRecords.put(current, record);
                            shardIteratorPerFailedMessage.remove(current);
                        }
                    }
                }
            }
        } catch (InterruptedException ie) {
            LOG.debug("Thread interrupted while sleeping", ie);
        } catch (ExpiredIteratorException ex) {
            LOG.debug("shardIterator for failedRecord " + kinesisMessageId + " has expired. Refreshing shardIterator");
            refreshShardIteratorForFailedRecord(kinesisMessageId);
        } catch (ProvisionedThroughputExceededException pe) {
            try {
                LOG.debug("ProvisionedThroughputExceededException occured. Check your limits. Sleeping for 1 second.", pe);
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.debug("Thread interrupted exception", e);
            }
        }
    }

    private void fetchNewRecords () {
        for (Map.Entry<String, LinkedList<Record>> entry : toEmitPerShard.entrySet()) {
            String shardId = entry.getKey();
            try {
                String shardIterator = shardIteratorPerShard.get(shardId);
                LOG.debug("Fetching new records for shard id :" + shardId + " using shardIterator " + shardIterator + " after sequence number " +
                        fetchedSequenceNumberPerShard.get(shardId));
                GetRecordsResult getRecordsResult = fetchRecords(shardIterator);
                if (getRecordsResult != null) {
                    List<Record> records = getRecordsResult.getRecords();
                    LOG.debug("Records size from fetchNewRecords is " + records.size());
                    // update the shard iterator to next one in case this fetch does not give the message.
                    shardIteratorPerShard.put(shardId, getRecordsResult.getNextShardIterator());
                    if (records.size() == 0) {
                        LOG.debug("No records returned from kinesis. Hence sleeping for 1 second");
                        Thread.sleep(1000);
                    } else {
                        entry.getValue().addAll(records);
                        fetchedSequenceNumberPerShard.put(shardId, records.get(records.size() - 1).getSequenceNumber());
                    }
                }
            } catch (InterruptedException ie) {
                LOG.debug("Thread interrupted while sleeping", ie);
            } catch (ExpiredIteratorException ex) {
                LOG.debug("shardIterator for shardId " + shardId + " has expired. Refreshing shardIterator");
                refreshShardIteratorForNewRecords(shardId);
            } catch (ProvisionedThroughputExceededException pe) {
                try {
                    LOG.debug("ProvisionedThroughputExceededException occured. Check your limits. Sleeping for 1 second.", pe);
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.debug("Thread interrupted exception", e);
                }
            }
        }
    }

    private GetRecordsResult fetchRecords (String shardIterator) {
        List<Record> records = new ArrayList<>();
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(shardIterator);
        getRecordsRequest.setLimit(config.getKinesisConnectionInfo().getRecordsLimit());
        GetRecordsResult getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
        return getRecordsResult;
    }

    private List<Shard> getShards () {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(config.getStreamName());
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
        } while ( exclusiveStartShardId != null );
        LOG.info("Number of shards for stream " + config.getStreamName() + " are " + shards.size());
        return shards;
    }

    private void emitNewRecord (SpoutOutputCollector collector) {
        for (Map.Entry<String, LinkedList<Record>> entry: toEmitPerShard.entrySet()) {
            String shardId = entry.getKey();
            LinkedList<Record> listOfRecords = entry.getValue();
            Record record;
            while ((record = listOfRecords.pollFirst()) != null) {
                KinesisMessageId kinesisMessageId = new KinesisMessageId(config.getStreamName(), shardId, record.getSequenceNumber());
                if (emitRecord(collector, record, kinesisMessageId)) {
                   return;
                }
            }
        }
    }

    private boolean emitFailedRecord (SpoutOutputCollector collector, KinesisMessageId kinesisMessageId) {
        if (!failedandFetchedRecords.containsKey(kinesisMessageId)) {
            return false;
        }
        return emitRecord(collector, failedandFetchedRecords.get(kinesisMessageId), kinesisMessageId);
    }

    private boolean emitRecord (SpoutOutputCollector collector, Record record, KinesisMessageId kinesisMessageId) {
        boolean result = false;
        List<Object> tuple = config.getRecordToTupleMapper().getTuple(record);
        // if a record is returned put the sequence number in the emittedPerShard to tie back with ack or fail
        if (tuple != null && tuple.size() > 0) {
            collector.emit(tuple, kinesisMessageId);
            if (!emittedPerShard.containsKey(kinesisMessageId.getShardId())) {
                emittedPerShard.put(kinesisMessageId.getShardId(), new TreeSet<BigInteger>());
            }
            emittedPerShard.get(kinesisMessageId.getShardId()).add(new BigInteger(record.getSequenceNumber()));
            result = true;
        } else {
            // ack to not process the record again on restart and move on to next message
            LOG.debug("Record " + record + " did not return a tuple to emit. Hence acking it");
            ack(kinesisMessageId);
        }
        return result;
    }

    private boolean shouldCommit () {
        return (System.currentTimeMillis() - lastCommitTime >= config.getZkInfo().getCommitIntervalMs());
    }

    private void initializeFetchedSequenceNumbers () {
        for (String shardId : toEmitPerShard.keySet()) {
            Map<Object, Object> state = readState(getZkPath(shardId));
            // if state found for this shard in zk, then set the sequence number in fetchedSequenceNumber
            if (state != null) {
                Object committedSequenceNumber = state.get("committedSequenceNumber");
                LOG.info("State read is committedSequenceNumber: " + committedSequenceNumber + " shardId:" + shardId);
                if (committedSequenceNumber != null) {
                    fetchedSequenceNumberPerShard.put(shardId, (String) committedSequenceNumber);
                }
            }
        }
    }

    private void refreshShardIteratorsForNewRecords () {
        for (String shardId: toEmitPerShard.keySet()) {
            refreshShardIteratorForNewRecords(shardId);
        }
    }

    private void refreshShardIteratorForNewRecords (String shardId) {
        String shardIterator = null;
        String lastFetchedSequenceNumber = fetchedSequenceNumberPerShard.get(shardId);
        ShardIteratorType shardIteratorType = (lastFetchedSequenceNumber == null ? config.getShardIteratorType() : ShardIteratorType
                .AFTER_SEQUENCE_NUMBER);
        // Set the shard iterator for last fetched sequence number to start from correct position in shard
        shardIterator = this.getShardIterator(shardId, shardIteratorType, lastFetchedSequenceNumber, config.getTimestamp());
        if (shardIterator != null && !shardIterator.isEmpty()) {
            LOG.debug("Refreshing shard iterator for new records for shardId " + shardId + " with shardIterator " + shardIterator);
            shardIteratorPerShard.put(shardId, shardIterator);
        }
    }

    private void refreshShardIteratorForFailedRecord (KinesisMessageId kinesisMessageId) {
        String shardIterator = null;
        // Set the shard iterator for last fetched sequence number to start from correct position in shard
        shardIterator = this.getShardIterator(kinesisMessageId.getShardId(), ShardIteratorType.AT_SEQUENCE_NUMBER, kinesisMessageId.getSequenceNumber(), null);
        if (shardIterator != null && !shardIterator.isEmpty()) {
            LOG.debug("Refreshing shard iterator for failed records for message " + kinesisMessageId + " with shardIterator " + shardIterator);
            shardIteratorPerFailedMessage.put(kinesisMessageId, shardIterator);
        }
    }

    private String getShardIterator (String shardId, ShardIteratorType shardIteratorType, String sequenceNumber, Date timestamp) {
        String shardIterator = "";
        try {
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            getShardIteratorRequest.setStreamName(config.getStreamName());
            getShardIteratorRequest.setShardId(shardId);
            getShardIteratorRequest.setShardIteratorType(shardIteratorType);
            if (shardIteratorType.equals(ShardIteratorType.AFTER_SEQUENCE_NUMBER) || shardIteratorType.equals(ShardIteratorType.AT_SEQUENCE_NUMBER)) {
                getShardIteratorRequest.setStartingSequenceNumber(sequenceNumber);
            } else if (shardIteratorType.equals(ShardIteratorType.AT_TIMESTAMP)) {
                getShardIteratorRequest.setTimestamp(timestamp);
            }
            GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
            if (getShardIteratorResult != null) {
                shardIterator = getShardIteratorResult.getShardIterator();
            }
        } catch (Exception e) {
            LOG.debug("Exception occured while getting shardIterator for shard " + shardId + " shardIteratorType " + shardIteratorType + " sequence number " +
                    sequenceNumber + " timestamp " + timestamp, e);
        }
        LOG.debug("Returning shardIterator " + shardIterator + " for shardId " + shardId + " shardIteratorType " + shardIteratorType + " sequenceNumber " +
                sequenceNumber + " timestamp" + timestamp);
        return shardIterator;
    }

    private Long getUncommittedRecordsCount () {
        Long result = 0L;
        for (Map.Entry<String, TreeSet<BigInteger>> emitted: emittedPerShard.entrySet()) {
            result += emitted.getValue().size();
        }
        for (Map.Entry<String, TreeSet<BigInteger>> acked: ackedPerShard.entrySet()) {
            result += acked.getValue().size();
        }
        for (Map.Entry<String, TreeSet<BigInteger>> failed: failedPerShard.entrySet()) {
            result += failed.getValue().size();
        }
        LOG.debug("Returning uncommittedRecordsCount as " + result);
        return result;
    }

    private boolean shouldFetchNewRecords () {
        // check to see if any shard has already fetched records waiting to be emitted, in which case dont fetch more
        boolean fetchRecords = true;
        for (Map.Entry<String, LinkedList<Record>> entry: toEmitPerShard.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                fetchRecords = false;
                break;
            }
        }
        return fetchRecords;
    }

    private void initializeCurator () {
        ZkInfo zkInfo = config.getZkInfo();
        curatorFramework = CuratorFrameworkFactory.newClient(zkInfo.getZkUrl(), zkInfo.getSessionTimeoutMs(), zkInfo.getConnectionTimeoutMs(), new
                RetryNTimes(zkInfo.getRetryAttempts(), zkInfo.getRetryIntervalMs()));
        curatorFramework.start();
    }

    private void initializeKinesisClient () {
        kinesisClient = new AmazonKinesisClient(config.getKinesisConnectionInfo().getCredentialsProvider(), config.getKinesisConnectionInfo().getClientConfiguration());
        kinesisClient.setRegion(Region.getRegion(config.getKinesisConnectionInfo().getRegion()));
    }

    private void shutdownCurator () {
        curatorFramework.close();
    }

    private void shutdownKinesisClient () {
        kinesisClient.shutdown();
    }

}
