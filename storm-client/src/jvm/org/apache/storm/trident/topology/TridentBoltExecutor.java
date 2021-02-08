/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.trident.topology;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.coordination.BatchOutputCollectorImpl;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.shade.org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.ReportedFailedException;
import org.apache.storm.trident.spout.IBatchID;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.utils.Utils;

public class TridentBoltExecutor implements IRichBolt {
    public static final String COORD_STREAM_PREFIX = "$coord-";
    Map<GlobalStreamId, String> batchGroupIds;
    Map<String, CoordSpec> coordSpecs;
    Map<String, CoordCondition> coordConditions;
    ITridentBatchBolt bolt;
    long messageTimeoutMs;
    long lastRotate;
    RotatingMap<Object, TrackedBatch> batches;
    OutputCollector collector;
    CoordinatedOutputCollector coordCollector;
    BatchOutputCollector coordOutputCollector;
    TopologyContext context;

    // map from batchgroupid to coordspec
    public TridentBoltExecutor(ITridentBatchBolt bolt, Map<GlobalStreamId, String> batchGroupIds,
                               Map<String, CoordSpec> coordinationSpecs) {
        this.batchGroupIds = batchGroupIds;
        coordSpecs = coordinationSpecs;
        this.bolt = bolt;
    }

    public static String coordStream(String batch) {
        return COORD_STREAM_PREFIX + batch;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        messageTimeoutMs = context.maxTopologyMessageTimeout() * 1000L;
        lastRotate = System.currentTimeMillis();
        batches = new RotatingMap<>(2);
        this.context = context;
        this.collector = collector;
        coordCollector = new CoordinatedOutputCollector(collector);
        coordOutputCollector = new BatchOutputCollectorImpl(new OutputCollector(coordCollector));

        coordConditions = (Map) context.getExecutorData("__coordConditions");
        if (coordConditions == null) {
            coordConditions = new HashMap<>();
            for (String batchGroup : coordSpecs.keySet()) {
                CoordSpec spec = coordSpecs.get(batchGroup);
                CoordCondition cond = new CoordCondition();
                cond.commitStream = spec.commitStream;
                cond.expectedTaskReports = 0;
                for (String comp : spec.coords.keySet()) {
                    CoordType ct = spec.coords.get(comp);
                    if (ct.equals(CoordType.single())) {
                        cond.expectedTaskReports += 1;
                    } else {
                        cond.expectedTaskReports += context.getComponentTasks(comp).size();
                    }
                }
                cond.targetTasks = new HashSet<>();
                for (String component : Utils.get(context.getThisTargets(),
                                                  coordStream(batchGroup),
                                                  new HashMap<String, Grouping>()).keySet()) {
                    cond.targetTasks.addAll(context.getComponentTasks(component));
                }
                coordConditions.put(batchGroup, cond);
            }
            context.setExecutorData("coordConditions", coordConditions);
        }
        bolt.prepare(conf, context, coordOutputCollector);
    }

    private void failBatch(TrackedBatch tracked, FailedException e) {
        if (e != null && e instanceof ReportedFailedException) {
            collector.reportError(e);
        }
        tracked.failed = true;
        if (tracked.delayedAck != null) {
            collector.fail(tracked.delayedAck);
            tracked.delayedAck = null;
        }
    }

    private void failBatch(TrackedBatch tracked) {
        failBatch(tracked, null);
    }

    private boolean finishBatch(TrackedBatch tracked, Tuple finishTuple) {
        boolean success = true;
        try {
            bolt.finishBatch(tracked.info);
            String stream = coordStream(tracked.info.batchGroup);
            for (Integer task : tracked.condition.targetTasks) {
                collector
                    .emitDirect(task, stream, finishTuple, new Values(tracked.info.batchId, Utils.get(tracked.taskEmittedTuples, task, 0)));
            }
            if (tracked.delayedAck != null) {
                collector.ack(tracked.delayedAck);
                tracked.delayedAck = null;
            }
        } catch (FailedException e) {
            failBatch(tracked, e);
            success = false;
        }
        batches.remove(tracked.info.batchId.getId());
        return success;
    }

    private void checkFinish(TrackedBatch tracked, Tuple tuple, TupleType type) {
        if (tracked.failed) {
            failBatch(tracked);
            collector.fail(tuple);
            return;
        }
        CoordCondition cond = tracked.condition;
        boolean delayed = tracked.delayedAck == null
                && (cond.commitStream != null && type == TupleType.COMMIT
                           || cond.commitStream == null);
        if (delayed) {
            tracked.delayedAck = tuple;
        }
        boolean failed = false;
        if (tracked.receivedCommit && tracked.reportedTasks == cond.expectedTaskReports) {
            if (tracked.receivedTuples == tracked.expectedTupleCount) {
                finishBatch(tracked, tuple);
            } else {
                //TODO: add logging that not all tuples were received
                failBatch(tracked);
                collector.fail(tuple);
                failed = true;
            }
        }

        if (!delayed && !failed) {
            collector.ack(tuple);
        }

    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            long now = System.currentTimeMillis();
            if (now - lastRotate > messageTimeoutMs) {
                batches.rotate();
                lastRotate = now;
            }
            return;
        }
        String batchGroup = batchGroupIds.get(tuple.getSourceGlobalStreamId());
        if (batchGroup == null) {
            // this is so we can do things like have simple DRPC that doesn't need to use batch processing
            coordCollector.setCurrBatch(null);
            bolt.execute(null, tuple);
            collector.ack(tuple);
            return;
        }
        IBatchID id = (IBatchID) tuple.getValue(0);
        //get transaction id
        //if it already exists and attempt id is greater than the attempt there

        TrackedBatch tracked = (TrackedBatch) batches.get(id.getId());

        // this code here ensures that only one attempt is ever tracked for a batch, so when
        // failures happen you don't get an explosion in memory usage in the tasks
        if (tracked != null) {
            if (id.getAttemptId() > tracked.attemptId) {
                batches.remove(id.getId());
                tracked = null;
            } else if (id.getAttemptId() < tracked.attemptId) {
                // no reason to try to execute a previous attempt than we've already seen
                return;
            }
        }

        if (tracked == null) {
            tracked =
                new TrackedBatch(new BatchInfo(batchGroup, id, bolt.initBatchState(batchGroup, id)), coordConditions.get(batchGroup),
                                 id.getAttemptId());
            batches.put(id.getId(), tracked);
        }
        coordCollector.setCurrBatch(tracked);

        //System.out.println("TRACKED: " + tracked + " " + tuple);

        TupleType t = getTupleType(tuple, tracked);
        if (t == TupleType.COMMIT) {
            tracked.receivedCommit = true;
            checkFinish(tracked, tuple, t);
        } else if (t == TupleType.COORD) {
            int count = tuple.getInteger(1);
            tracked.reportedTasks++;
            tracked.expectedTupleCount += count;
            checkFinish(tracked, tuple, t);
        } else {
            tracked.receivedTuples++;
            boolean success = true;
            try {
                bolt.execute(tracked.info, tuple);
                if (tracked.condition.expectedTaskReports == 0) {
                    success = finishBatch(tracked, tuple);
                }
            } catch (FailedException e) {
                failBatch(tracked, e);
            }
            if (success) {
                collector.ack(tuple);
            } else {
                collector.fail(tuple);
            }
        }
        coordCollector.setCurrBatch(null);
    }

    @Override
    public void cleanup() {
        bolt.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        bolt.declareOutputFields(declarer);
        for (String batchGroup : coordSpecs.keySet()) {
            declarer.declareStream(coordStream(batchGroup), true, new Fields("id", "count"));
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> ret = bolt.getComponentConfiguration();
        if (ret == null) {
            ret = new HashMap<>();
        }
        ret.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        // TODO: Need to be able to set the tick tuple time to the message timeout, ideally without parameterization
        return ret;
    }

    private TupleType getTupleType(Tuple tuple, TrackedBatch batch) {
        CoordCondition cond = batch.condition;
        if (cond.commitStream != null
            && tuple.getSourceGlobalStreamId().equals(cond.commitStream)) {
            return TupleType.COMMIT;
        } else if (cond.expectedTaskReports > 0
                   && tuple.getSourceStreamId().startsWith(COORD_STREAM_PREFIX)) {
            return TupleType.COORD;
        } else {
            return TupleType.REGULAR;
        }
    }

    enum TupleType {
        REGULAR,
        COMMIT,
        COORD
    }

    public static class CoordType implements Serializable {
        public boolean singleCount;

        protected CoordType(boolean singleCount) {
            this.singleCount = singleCount;
        }

        public static CoordType single() {
            return new CoordType(true);
        }

        public static CoordType all() {
            return new CoordType(false);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CoordType)) {
                return false;
            }

            CoordType coordType = (CoordType) o;

            return singleCount == coordType.singleCount;
        }

        @Override
        public int hashCode() {
            return (singleCount ? 1 : 0);
        }

        @Override
        public String toString() {
            return "<Single: " + singleCount + ">";
        }
    }

    public static class CoordSpec implements Serializable {
        public GlobalStreamId commitStream = null;
        public Map<String, CoordType> coords = new HashMap<>();

        public CoordSpec() {
        }
    }

    public static class CoordCondition implements Serializable {
        public GlobalStreamId commitStream;
        public int expectedTaskReports;
        Set<Integer> targetTasks;

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    public static class TrackedBatch {
        int attemptId;
        BatchInfo info;
        CoordCondition condition;
        int reportedTasks = 0;
        int expectedTupleCount = 0;
        int receivedTuples = 0;
        Map<Integer, Integer> taskEmittedTuples = new HashMap<>();
        boolean failed = false;
        boolean receivedCommit;
        Tuple delayedAck = null;

        public TrackedBatch(BatchInfo info, CoordCondition condition, int attemptId) {
            this.info = info;
            this.condition = condition;
            this.attemptId = attemptId;
            receivedCommit = condition.commitStream == null;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    private static class CoordinatedOutputCollector implements IOutputCollector {
        IOutputCollector delegate;

        TrackedBatch currBatch = null;

        CoordinatedOutputCollector(IOutputCollector delegate) {
            this.delegate = delegate;
        }

        public void setCurrBatch(TrackedBatch batch) {
            currBatch = batch;
        }

        @Override
        public List<Integer> emit(String stream, Collection<Tuple> anchors, List<Object> tuple) {
            List<Integer> tasks = delegate.emit(stream, anchors, tuple);
            updateTaskCounts(tasks);
            return tasks;
        }

        @Override
        public void emitDirect(int task, String stream, Collection<Tuple> anchors, List<Object> tuple) {
            updateTaskCounts(Arrays.asList(task));
            delegate.emitDirect(task, stream, anchors, tuple);
        }

        @Override
        public void ack(Tuple tuple) {
            throw new IllegalStateException("Method should never be called");
        }

        @Override
        public void fail(Tuple tuple) {
            throw new IllegalStateException("Method should never be called");
        }

        @Override
        public void resetTimeout(Tuple tuple) {
            throw new IllegalStateException("Method should never be called");
        }

        @Override
        public void flush() {
            delegate.flush();
        }

        @Override
        public void reportError(Throwable error) {
            delegate.reportError(error);
        }


        private void updateTaskCounts(List<Integer> tasks) {
            if (currBatch != null) {
                Map<Integer, Integer> taskEmittedTuples = currBatch.taskEmittedTuples;
                for (Integer task : tasks) {
                    int newCount = Utils.get(taskEmittedTuples, task, 0) + 1;
                    taskEmittedTuples.put(task, newCount);
                }
            }
        }
    }
}
