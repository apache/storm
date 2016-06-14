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

import java.io.Serializable;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Kinesis Spout configuration.
 */
public class KinesisSpoutConfig implements Serializable {
    private static final long serialVersionUID = 3528909581809795597L;

    private final String streamName;
    private int maxRecordsPerCall = 10000;
    private InitialPositionInStream initialPositionInStream = InitialPositionInStream.LATEST;
    private int checkpointIntervalMillis = 60000;
    // Backoff time between Kinesis GetRecords API calls (per shard) when a call returns an empty list of records.
    private long emptyRecordListBackoffMillis = 500L;
    private int recordRetryLimit = 3;
    private Regions region = Regions.US_EAST_1;

    private final String zookeeperConnectionString;
    private String zookeeperPrefix = "kinesis_storm_spout";
    private int zookeeperSessionTimeoutMillis = 10000;

    private IKinesisRecordScheme scheme = new DefaultKinesisRecordScheme();

    // Gets set by the spout later on.
    private String topologyName = "UNNAMED_TOPOLOGY";

    public KinesisSpoutConfig(final String streamName, final String zookeeperConnectionString) {
        this.streamName = streamName;
        this.zookeeperConnectionString = zookeeperConnectionString;
    }

    /**
     * Constructor.
     * 
     * @param streamName Name of the Kinesis stream.
     * @param maxRecordsPerCall Max number records to fetch from Kinesis in a single GetRecords call
     * @param initialPositionInStream Fetch records from this position if a checkpoint doesn't exist
     * @param zookeeperPrefix Prefix for Zookeeper paths when storing spout state.
     * @param zookeeperConnectionString Endpoint for connecting to ZooKeeper (e.g. "localhost:2181").
     * @param zookeeperSessionTimeoutMillis Timeout for ZK session.
     * @param checkpointIntervalMillis Save checkpoint to Zookeeper this often.
     * @param scheme Used to convert a Kinesis record into a tuple
     */
    public KinesisSpoutConfig(final String streamName,
            final int maxRecordsPerCall,
            final InitialPositionInStream initialPositionInStream,
            final String zookeeperPrefix,
            final String zookeeperConnectionString,
            final int zookeeperSessionTimeoutMillis,
            final int checkpointIntervalMillis,
            final IKinesisRecordScheme scheme) {
        this.scheme = scheme;
        this.streamName = streamName;
        checkValueIsPositive(maxRecordsPerCall, "maxRecordsPerCall");
        this.maxRecordsPerCall = maxRecordsPerCall;
        this.initialPositionInStream = initialPositionInStream;
        this.zookeeperPrefix = zookeeperPrefix;
        this.zookeeperConnectionString = zookeeperConnectionString;
        checkValueIsPositive(zookeeperSessionTimeoutMillis, "zookeeperSessionTimeoutMillis");
        this.zookeeperSessionTimeoutMillis = zookeeperSessionTimeoutMillis;
        checkValueIsPositive(checkpointIntervalMillis, "checkpointIntervalMillis");
        this.checkpointIntervalMillis = checkpointIntervalMillis;
    }

    private void checkValueIsPositive(int argument, String argumentName) {
        if (argument <= 0) {
            throw new IllegalArgumentException("Value of " + argumentName + " must be positive, but was " + argument);
        }
    }

    private void checkValueIsNotNegative(int argument, String argumentName) {
        if (argument < 0) {
            throw new IllegalArgumentException("Value of " + argumentName + " must be >= 0, but was " + argument);
        }
    }
    
    private void checkValueIsNotNull(Object object, String argumentName) {
        if (object == null) {
            throw new IllegalArgumentException(argumentName + " should not be null");
        }
    }

    /**
     * @return Scheme used to convert a Kinesis record to a tuple.
     */
    public IKinesisRecordScheme getScheme() {
        return scheme;
    }

    /**
     * @param scheme Scheme used to convert a Kinesis record to a tuple.
     * @return KinesisSpoutConfig
     */
    public KinesisSpoutConfig withKinesisRecordScheme(IKinesisRecordScheme scheme) {
        this.scheme = scheme;
        return this;
    }

    /**
     * @return Prefix used when storing spout state in Zookeeper.
     */
    public String getZookeeperPrefix() {
        return zookeeperPrefix;
    }

    /**
     * @param prefix Prefix (path) used when storing spout state in Zookeeper.
     * @return KinesisSpoutConfig
     */
    public KinesisSpoutConfig withZookeeperPrefix(String prefix) {
        this.zookeeperPrefix = prefix;
        return this;
    }

    /**
     * @return Zookeeper connection string.
     */
    public String getZookeeperConnectionString() {
        return zookeeperConnectionString;
    }

    /**
     * @return Zookeeper session timeout.
     */
    public int getZookeeperSessionTimeoutMillis() {
        return zookeeperSessionTimeoutMillis;
    }

    /**
     * @param zookeeperSessionTimeoutMillis Zookeeper session timeout
     * @return KinesisSpoutConfig
     */
    public KinesisSpoutConfig withZookeeperSessionTimeoutMillis(int zookeeperSessionTimeoutMillis) {
        checkValueIsPositive(zookeeperSessionTimeoutMillis, "zookeeperSessionTimeoutMillis");
        this.zookeeperSessionTimeoutMillis = zookeeperSessionTimeoutMillis;
        return this;
    }

    /**
     * @return Checkpoint interval (e.g. checkpoint every 30 seconds).
     */
    public int getCheckpointIntervalMillis() {
        return checkpointIntervalMillis;
    }

    /**
     * @param checkpointIntervalMillis Save checkpoint to Zookeeper this often.
     * @return KinesisSpoutConfig
     */
    public KinesisSpoutConfig withCheckpointIntervalMillis(int checkpointIntervalMillis) {
        checkValueIsPositive(checkpointIntervalMillis, "checkpointIntervalMillis");
        this.checkpointIntervalMillis = checkpointIntervalMillis;
        return this;
    }

    /**
     * @return Name of the Kinesis stream.
     */
    public String getStreamName() {
        return streamName;
    }

    /**
     * @return Name of the topology.
     */
    public String getTopologyName() {
        return topologyName;
    }

    /**
     * @param topologyName Name of the topology.
     */
    public void setTopologyName(final String topologyName) {
        this.topologyName = topologyName;
    }

    /**
     * @return the initialPositionInStream
     */
    public InitialPositionInStream getInitialPositionInStream() {
        return initialPositionInStream;
    }

    /**
     * @param initialPosition Fetch records from this position if a checkpoint doesn't exist
     * @return KinesisSpoutConfig
     */
    public KinesisSpoutConfig withInitialPositionInStream(InitialPositionInStream initialPosition) {
        this.initialPositionInStream = initialPosition;
        return this;
    }

    /**
     * @return maxRecordsPerCall (GetRecords API call)
     */
    public int getMaxRecordsPerCall() {
        return maxRecordsPerCall;
    }

    /**
     * @param maxRecordsPerCall Max number records to fetch from Kinesis in a single GetRecords call
     * @return KinesisSpoutConfig
     */
    public KinesisSpoutConfig withMaxRecordsPerCall(int maxRecordsPerCall) {
        checkValueIsPositive(maxRecordsPerCall, "maxRecordsPerCall");
        this.maxRecordsPerCall = maxRecordsPerCall;
        return this;
    }

    /**
     * @return recordRetryLimit Max retry attempts for a record (upon failure).
     */
    public int getRecordRetryLimit() {
        return recordRetryLimit;
    }
    
    /**
     * @param recordRetryLimit Max retry attempts for a record (upon failure)
     * @return KinesisSpoutConfig
     */
    public KinesisSpoutConfig withRecordRetryLimit(int recordRetryLimit) {
        checkValueIsNotNegative(recordRetryLimit, "recordRetryLimit");
        this.recordRetryLimit = recordRetryLimit;
        return this;
    }

    /**
     * @return the region
     */
    public Region getRegion() {
        return Region.getRegion(region);
    }

    /**
     * @param region AWS Region
     * @return KinesisSpoutConfig
     */
    public KinesisSpoutConfig withRegion(Regions region) {
        checkValueIsNotNull(region, "region");
        this.region = region;
        return this;
    }

    public long getEmptyRecordListBackoffMillis() {
        return emptyRecordListBackoffMillis ;
    }
    
    /**
     * @param emptyRecordListBackoffMillis Backoff for this time before making the next Kinesis GetRecords API call (per
     *        shard) if the previous call returned no records.
     * @return KinesisSpoutConfig
     */
    public KinesisSpoutConfig withEmptyRecordListBackoffMillis(long emptyRecordListBackoffMillis) {
        this.emptyRecordListBackoffMillis = emptyRecordListBackoffMillis;
        return this;
    }
}
