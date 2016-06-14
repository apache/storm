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

package org.apache.storm.kinesis.spout.state.zookeeper;

import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.kinesis.spout.KinesisSpoutConfig;
import org.apache.storm.kinesis.spout.exceptions.KinesisSpoutException;
import org.apache.storm.kinesis.spout.state.zookeeper.NodeFunction.Mod;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Handles communication with Zookeeper and methods specific to the spout for saving/restoring
 * state.
 */
class ZookeeperShardState {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperShardState.class);
    private static final int BASE_SLEEP_TIME_MS = 200;
    private static final int BASE_OPTIMISTIC_RETRY_TIME_MS = 100;
    private static final int MAX_NUM_RETRIES = 5;
    private static final String SHARD_LIST_SUFFIX = "shardList";
    private static final String STATE_SUFFIX = "state";

    private final KinesisSpoutConfig config;
    private final Random rand;
    private final CuratorFramework zk;

    /**
     * Create and configure the ZK sync object with the KinesisSpoutConfig.
     * @param config  the configuration for the spout.
     */
    ZookeeperShardState(final KinesisSpoutConfig config) {
        this.config = config;
        this.rand = new Random();

        zk = CuratorFrameworkFactory.newClient(config.getZookeeperConnectionString(),
                new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_NUM_RETRIES));
        zk.start();
    }

    /**
     * Initialize the shardList in ZK. This is called by every spout task on activate(), and ensures
     * that the shardList is up to date and correct.
     *
     * @param shards  list of shards (output of DescribeStream).
     * @throws Exception
     */
    void initialize(final ImmutableList<String> shards) throws Exception {
        NodeFunction verifyOrCreateShardList = new NodeFunction() {
            @Override
            public byte[] initialize() {
                LOG.info(this + " First initialization of shardList: " + shards);
                ShardListV0 shardList = new ShardListV0(shards);
                ObjectMapper objectMapper = new ObjectMapper();
                byte[] data;
                try {
                    data = objectMapper.writeValueAsBytes(shardList);
                } catch (JsonProcessingException e) {
                    throw new KinesisSpoutException("Unable to serialize shardList " + shardList, e);
                }
                return data;
            }

            @Override
            public Mod<byte[]> apply(byte[] x) {
                // At this point, we don't support resharding. We assume the shard list is valid if one exists.
                LOG.info("ShardList already initialized in Zookeeper. Assuming it is valid.");
                return Mod.noModification();
            }
        };

        atomicUpdate(SHARD_LIST_SUFFIX, verifyOrCreateShardList);
    }

    /**
     * Delete the shard list in ZK. This is called by every spout task on deactivate(), so that
     * when the task is reactivated, the latest shard list is retrieved.
     *
     * @throws Exception
     */
    void clearShardList() throws Exception {
        delete(SHARD_LIST_SUFFIX);
    }

    /**
     * Commit the checkpoint sequence number for a shard to Zookeeper.
     *
     * @param  shardId  shard to commit to.
     * @param  seqNum  sequence number to commit.
     * @throws Exception
     */
    void commitSeqNum(final String shardId, final String seqNum) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] data = objectMapper.writeValueAsBytes(new ShardStateV0(seqNum));
        NodeFunction commit = NodeFunction.constant(data);
        atomicUpdate(shardId + "/" + STATE_SUFFIX, commit);
    }

    /**
     * Get the last committed sequence number for the shard from Zookeeper.
     *
     * @param  shardId  shard to read from.
     * @return a sequence number if the state exists, empty string otherwise.
     * @throws Exception
     */
    String getLastCommittedSeqNum(final String shardId) throws Exception {
        try {
            byte[] data = get(shardId + "/" + STATE_SUFFIX);
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(data, ShardStateV0.class).getCheckpoint();
        } catch (KeeperException.NoNodeException e) {
            LOG.info("No shard state for " + shardId);
            return "";
        }
    }

    /**
     * Get the shardList from ZK. The spout should do assignments based on this list, not
     * the one returned by DescribeStream. This will ensure consistency across all spout tasks.
     *
     * @return the list of shards in the stream.
     * @throws Exception
     */
    ImmutableList<String> getShardList() throws Exception {
        byte[] data = get(SHARD_LIST_SUFFIX);
        ObjectMapper objectMapper = new ObjectMapper();
        return ImmutableList.copyOf(objectMapper.readValue(data, ShardListV0.class).getShardList());
    }

    /**
     * Set a watcher for the shardList.
     * 
     * @param callback Zookeeper watcher to be set on the shard list.
     * @throws Exception
     */
    void watchShardList(Watcher callback) throws Exception {
        watch(SHARD_LIST_SUFFIX, callback);
    }

    /**
     * @param path path to check.
     * @return true if path is the path to the shard list.
     */
    boolean isShardList(final String path) {
        return buildZookeeperPath(SHARD_LIST_SUFFIX).equals(path);
    }

    /**
     * Closes the connection to ZK.
     * 
     * @throws InterruptedException
     */
    void close() throws InterruptedException {
        zk.close();
    }

    /**
     * Optimistic concurrency scheme for tryAtomicUpdate. Try to update, and keep trying
     * until successful.
     * 
     * @param pathSuffix suffix to use to build path in ZooKeeper.
     * @param f function used to initialize the node, or transform the data already there.
     * @throws Exception
     */
    private void atomicUpdate(final String pathSuffix, final NodeFunction f) throws Exception {
        boolean done = false;

        do {
            done = RetryLoop.callWithRetry(zk.getZookeeperClient(), new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return tryAtomicUpdate(pathSuffix, f);
                }
            });
            Thread.sleep(BASE_OPTIMISTIC_RETRY_TIME_MS
                    + rand.nextInt(BASE_OPTIMISTIC_RETRY_TIME_MS));
        } while(!done);
    }

    private byte[] get(final String pathSuffix) throws Exception {
        return RetryLoop.callWithRetry(zk.getZookeeperClient(), new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                return zk.getData().forPath(buildZookeeperPath(pathSuffix));
            }
        });
    }

    private void delete(final String pathSuffix) throws Exception {
        RetryLoop.callWithRetry(zk.getZookeeperClient(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    zk.delete().forPath(buildZookeeperPath(pathSuffix));
                    return null;
                } catch (KeeperException.NoNodeException e) {
                    // likely deleted by another task
                    return null;
                }
            }
        });
    }

    private void watch(final String pathSuffix, final Watcher callback) throws Exception {
        RetryLoop.callWithRetry(zk.getZookeeperClient(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                zk.checkExists().usingWatcher(callback).forPath(buildZookeeperPath(pathSuffix));
                return null;
            }
        });
    }

    /**
     * Try to atomically update a node in ZooKeeper, creating it if it doesn't exist. This is
     * meant to be used within an optimistic concurrency model.
     * 
     * @param pathSuffix suffix to use to build path in ZooKeeper.
     * @param f function used to initialize the node, or transform the data already there.
     * @return true if node was created/updated, false if a concurrent modification occurred
     *         and succeeded while trying to update/create the node.
     * @throws Exception
     */
    private boolean tryAtomicUpdate(final String pathSuffix, final NodeFunction f) throws Exception {
        final String path = buildZookeeperPath(pathSuffix);
        final Stat stat = zk.checkExists().forPath(path);

        if (stat == null) {
            try {
                zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                  .forPath(path, f.initialize());
            } catch (KeeperException.NodeExistsException e) {
                LOG.debug("Concurrent creation of " + path + ", retrying", e);
                return false;
            }
        } else {
            Mod<byte[]> newVal = f.apply(zk.getData().forPath(path));

            if (newVal.hasModification()) {
                try {
                    zk.setData().withVersion(stat.getVersion()).forPath(path, newVal.get());
                } catch (KeeperException.BadVersionException e) {
                    LOG.debug("Concurrent update to " + path + ", retrying.", e);
                    return false;
                }
            }
        }

        return true;
    }

    private String buildZookeeperPath(final String suffix) {
        return "/" + config.getZookeeperPrefix() + "/" + config.getTopologyName() + "/"
               +  config.getStreamName() + "/" + suffix;
    }
}
