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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.storm.kinesis.spout.IShardGetter;
import org.apache.storm.kinesis.spout.IShardGetterBuilder;
import org.apache.storm.kinesis.spout.IShardListGetter;
import org.apache.storm.kinesis.spout.InitialPositionInStream;
import org.apache.storm.kinesis.spout.KinesisSpoutConfig;
import org.apache.storm.kinesis.spout.ShardPosition;
import org.apache.storm.kinesis.spout.exceptions.InvalidSeekPositionException;
import org.apache.storm.kinesis.spout.exceptions.KinesisSpoutException;
import org.apache.storm.kinesis.spout.state.IKinesisSpoutStateManager;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.model.Record;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

/**
 * Zookeeper backed IKinesisSpoutStateManager.
 */
public class ZookeeperStateManager implements Watcher, IKinesisSpoutStateManager {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperStateManager.class);

    private final KinesisSpoutConfig config;
    private final IShardListGetter shardListGetter;
    private final IShardGetterBuilder getterBuilder;
    private final ShardPosition seekToOnOpen;

    private ZookeeperShardState zk;
    private int taskIndex;
    private int totalNumTasks;
    private boolean active;

    private ImmutableList<IShardGetter> getters;
    private Iterator<IShardGetter> currentGetter;
    private Map<String, LocalShardState> shardStates;

    /**
     * @param config Spout configuration with ZK preferences.
     * @param shardListGetter Used to fetch the list of shards in the stream.
     * @param getterBuilder Used to build getters for shards a task is responsible for.
     * @param initialPosition Fetch records from this position when there is no pre-existing ZK state.
     */
    public ZookeeperStateManager(
            final KinesisSpoutConfig config,
            final IShardListGetter shardListGetter,
            final IShardGetterBuilder getterBuilder,
            final InitialPositionInStream initialPosition) {
        this.config = config;
        this.shardListGetter = shardListGetter;
        this.getterBuilder = getterBuilder;
        this.seekToOnOpen = getShardPosition(initialPosition);
        this.active = false;
    }

    private ShardPosition getShardPosition(InitialPositionInStream initialPosition) {
        ShardPosition position = null;
        if (initialPosition.equals(InitialPositionInStream.TRIM_HORIZON)) {
            position = ShardPosition.trimHorizon();
        } else if (initialPosition.equals(InitialPositionInStream.LATEST)) {
            position = ShardPosition.end();
        } else {
            throw new IllegalArgumentException("Initial position must be one of TRIM_HORIZON or LATEST, but was "
                    + initialPosition.toString());
        }
        return position;
    }

    @Override
    public void activate() {
        this.zk = new ZookeeperShardState(config);
        this.active = true;

        // Ensure that the task can safely be activated
        // This will take care of making sure the list is sorted too.
        ImmutableList<String> shardList = ImmutableList.copyOf(shardListGetter.getShardList().keySet());
        LOG.info(this + "Activating with shardList " + shardList);
        try {
            zk.initialize(shardList);
            // Hook shardList watcher for the first time.
            zk.watchShardList(this);
        } catch (Exception e) {
            LOG.error(this + " something went wrong while initializing Zookeeper shardList."
                      + " Assuming it is unsafe to continue.", e);
            throw new KinesisSpoutException(e);
        }
    }

    @Override
    public void deactivate() throws InterruptedException {
        commitShardStates();

        this.active = false;
        try {
            zk.clearShardList();
        } catch (Exception e) {
            LOG.error(this + " something went wrong while clearing Zookeeper shard list.", e);
        }
        zk.close();
    }

    @Override
    public IShardGetter getNextGetter() {
        assert hasGetters();
        return currentGetter.next();
    }

    @Override
    public boolean hasGetters() {
        return currentGetter.hasNext();
    }

    @Override
    public void rebalance(final int newTaskIndex, final int newTotalNumTasks) {
        checkState(active, "Cannot rebalance if state is not active (a ZK connection"
                + " is necessary).");

        this.taskIndex = newTaskIndex;
        this.totalNumTasks = newTotalNumTasks;

        commitShardStates();
        bootstrapStateFromZookeeper();
    }

    @Override
    public void ack(final String shardId, final String seqNum) {
        final LocalShardState st = shardStates.get(shardId);

        // If st is null, then task assignment changed and this shard is no longer
        // handled by this task. In this case, the checkpoint will have been
        // written to ZK and another task will have replayed all messages since.
        // It is no longer this task's job to take care of this ack - ignore it.
        if (st != null) {
            st.ack(seqNum);
            st.logMe(this + "[ACK] shard state for " + shardId + " after " + seqNum + " ");
        }
    }

    @Override
    public void fail(final String shardId, final String seqNum) {
        final LocalShardState st = shardStates.get(shardId);

        // Process only if this task is still responsible for the shard.
        if (st != null) {
            st.fail(seqNum);
            st.logMe(this + "[FAIL] shard state for " + shardId + " after " + seqNum + " ");
        }
    }

    @Override
    public void emit(final String shardId, final Record record, boolean isRetry) {
        safeGetShardState(shardId).emit(record, isRetry);
    }

    @Override
    public boolean shouldRetry(final String shardId) {
        return safeGetShardState(shardId).shouldRetry();
    }

    @Override
    public Record recordToRetry(final String shardId) {
        return safeGetShardState(shardId).recordToRetry();
    }

    // Will commit the checkpoint from the local shard states to ZK if the ZK
    // state needs updating.
    @Override
    public void commitShardStates() {
        checkState(active, "Cannot commit state if state is not active (a ZK"
                + " connection is necessary).");

        if (shardStates == null) {
            LOG.debug(this + " Null shard states nothing to commit");
            return;
        }

        for (final Entry<String, LocalShardState> entry : shardStates.entrySet()) {
            final String shardId = entry.getKey();
            final LocalShardState st = entry.getValue();

            if (st.isDirty()) {
                try {
                    String checkpointSequenceNumber = st.getLatestValidSeqNum();
                    zk.commitSeqNum(shardId, checkpointSequenceNumber);
                    st.commit(checkpointSequenceNumber);
                    LOG.info(this + "Advanced checkpoint for " + shardId + " to " + st.getLatestValidSeqNum());
                } catch (Exception e) {
                    String message = this + " could not commit ZK state for shardId=" + shardId + "."
                            + " The ZK state is now out of date.";
                    LOG.error(message, e);
                }
            } else {
                LOG.debug(this + "Local shard state for " + shardId + " was not dirty - not doing anything");
            }
        }
    }

    // fail, ack and nextTuple all run within the same thread but process runs on a separate thread.
    // Since they all modify the same state, they must be synchronized.
    // They are all synchronized on the instance of this class.
    @Override
    public synchronized void process(WatchedEvent event) {
        checkState(active, "Cannot process events if state is not active (a ZK"
                + " connection is necessary).");

        // Re-hook the watcher.
        // Ordering the statements this way will ensure that no ZK shardList update is missed.
        // Since bootstrapStateFromZookeeper does its own getShardList(null) call, and the
        // call is after the re-hook, then any modification made in between the time the process()
        // function is called and the re-hook is made is captured by bootstrapStateFromZookeeper.
        // Note that this could end up in multiple calls being made for the same shardList update.
        try {
            zk.watchShardList(this);
        } catch (Exception e) {
            // Failure is fatal for the task (and it's been retried, so it's indicative of a
            // bigger Zookeeper/global state issue).
            LOG.error(this + " failure to re-attach event handler for ZK node "
                      + event.getPath(), e);
            throw new KinesisSpoutException(e);
        }

        // If we are handling a shardList modification, then most probably another task handled
        // a reshard, and we need to sync with the state in ZK.
        if (event.getType() == EventType.NodeDataChanged && zk.isShardList(event.getPath())) {
            LOG.info(this + " detected change in shardList. Committing current shard state and "
                     + "reinitializing spout task from ZK.");

            commitShardStates();
            bootstrapStateFromZookeeper();
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append("taskIndex", taskIndex)
            .toString();
    }

    // Recomputes shard assignment for the current task. Then, recreates the local shard state
    // and the getters from whatever data is in Zookeeper.
    private void bootstrapStateFromZookeeper() {
        ImmutableList<String> shardAssignment = getShardAssignment();

        // Task could not get an assignment (e.g. there are too many tasks for too few shards).
        if (shardAssignment.isEmpty()) {
            this.shardStates = new HashMap<>();
            this.getters = ImmutableList.of();
        } else {
            this.shardStates = makeLocalState(shardAssignment);
            this.getters = makeGetters(shardAssignment);
        }

        this.currentGetter = Iterators.cycle(getters);
        LOG.info(this + " got getter assignment. Handling " + getters + ".");
    }

    // Create the local shard state from Zookeeper.
    private Map<String, LocalShardState> makeLocalState(ImmutableList<String> shardAssignment) {
        Map<String, LocalShardState> state = new HashMap<>();

        for (final String shardId : shardAssignment) {
            String latestValidSeqNum;
            try {
                latestValidSeqNum = zk.getLastCommittedSeqNum(shardId);
            } catch (Exception e) {
                LOG.error(this + " could not retrieve last committed seqnum for " + shardId
                          + " from ZooKeeper. Starting from default getter position.");
                latestValidSeqNum = "";
            }
            state.put(shardId, new LocalShardState(shardId, latestValidSeqNum, config.getRecordRetryLimit()));
        }

        return state;
    }

    // Opens getters based on shard assignment and local shard state, and seeks them to seekToOnOpen.
    private ImmutableList<IShardGetter> makeGetters(ImmutableList<String> shardAssignment) {
        // Pre : shardList is initialized.
        assert shardStates != null && !shardStates.isEmpty();

        final ImmutableList<IShardGetter> myGetters = getterBuilder.buildGetters(shardAssignment);

        for (final IShardGetter getter: myGetters) {
            final String shardId = getter.getAssociatedShard();
            final LocalShardState shardState = safeGetShardState(shardId);

            try {
                if (shardState.getLatestValidSeqNum().isEmpty() && seekToOnOpen != null) {
                    getter.seek(seekToOnOpen);
                } else if (!shardState.getLatestValidSeqNum().isEmpty()) {
                    getter.seek(ShardPosition.afterSequenceNumber(
                            shardState.getLatestValidSeqNum()));
                }
            } catch (InvalidSeekPositionException e) {
                LOG.error(this + " tried to seek getter " + getter + " to an invalid position.", e);
                throw new KinesisSpoutException("Could not seek getter for " + shardId, e);
            }
        }

        return myGetters;
    }

    // Computes the task's shard assignment based on the task index and the total number of tasks.
    private ImmutableList<String> getShardAssignment() {
        final ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        ImmutableList<String> shardList;

        // Note that this uses ZK, not DescribeStream API. This ensures that all
        // tasks share a consistent (although possibly outdated) view of the stream.
        try {
            shardList = zk.getShardList();
            LOG.info(this + " Got shardList: " + shardList);
        } catch (Exception e) {
            LOG.error(this + " could not compute shard assigment: could not retrieve shard list"
                      + " from ZK.", e);
            throw new KinesisSpoutException(e);
        }

        for (int i = taskIndex; i < shardList.size(); i += totalNumTasks) {
            builder.add(shardList.get(i));
        }

        return builder.build();
    }

    // Post : getShardState(_) != null
    private LocalShardState safeGetShardState(final String shardId) {
        final LocalShardState st = shardStates.get(shardId);
        checkNotNull(st, "Shard state map inconsistent with shard assignment (could not get"
                         + " shardId=" + shardId + ").");
        return st;
    }
}
