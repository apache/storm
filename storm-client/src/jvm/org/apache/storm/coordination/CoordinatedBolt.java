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

package org.apache.storm.coordination;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.storm.Constants;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TimeCacheMap;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordination requires the request ids to be globally unique for awhile. This is so it doesn't get confused in the case of retries.
 */
public class CoordinatedBolt implements IRichBolt {
    public static final Logger LOG = LoggerFactory.getLogger(CoordinatedBolt.class);
    private Map<String, SourceArgs> sourceArgs;
    private IdStreamSpec idStreamSpec;
    private IRichBolt delegate;
    private Integer numSourceReports;
    private List<Integer> countOutTasks = new ArrayList<>();
    private OutputCollector collector;
    private TimeCacheMap<Object, TrackingInfo> tracked;

    public CoordinatedBolt(IRichBolt delegate) {
        this(delegate, null, null);
    }

    public CoordinatedBolt(IRichBolt delegate, String sourceComponent, SourceArgs sourceArgs, IdStreamSpec idStreamSpec) {
        this(delegate, singleSourceArgs(sourceComponent, sourceArgs), idStreamSpec);
    }

    public CoordinatedBolt(IRichBolt delegate, Map<String, SourceArgs> sourceArgs, IdStreamSpec idStreamSpec) {
        this.sourceArgs = sourceArgs;
        if (this.sourceArgs == null) {
            this.sourceArgs = new HashMap<>();
        }
        this.delegate = delegate;
        this.idStreamSpec = idStreamSpec;
    }

    private static Map<String, SourceArgs> singleSourceArgs(String sourceComponent, SourceArgs sourceArgs) {
        Map<String, SourceArgs> ret = new HashMap<>();
        ret.put(sourceComponent, sourceArgs);
        return ret;
    }

    @Override
    public void prepare(Map<String, Object> config, TopologyContext context, OutputCollector collector) {
        TimeCacheMap.ExpiredCallback<Object, TrackingInfo> callback = null;
        if (delegate instanceof TimeoutCallback) {
            callback = new TimeoutItems();
        }
        tracked = new TimeCacheMap<>(context.maxTopologyMessageTimeout(), callback);
        this.collector = collector;
        delegate.prepare(config, context, new OutputCollector(new CoordinatedOutputCollector(collector)));
        for (String component : Utils.get(context.getThisTargets(),
                                          Constants.COORDINATED_STREAM_ID,
                                          new HashMap<String, Grouping>())
                                     .keySet()) {
            for (Integer task : context.getComponentTasks(component)) {
                countOutTasks.add(task);
            }
        }
        if (!sourceArgs.isEmpty()) {
            numSourceReports = 0;
            for (Entry<String, SourceArgs> entry : sourceArgs.entrySet()) {
                if (entry.getValue().singleCount) {
                    numSourceReports += 1;
                } else {
                    numSourceReports += context.getComponentTasks(entry.getKey()).size();
                }
            }
        }
    }

    private boolean checkFinishId(Tuple tup, TupleType type) {
        Object id = tup.getValue(0);
        boolean failed = false;

        synchronized (tracked) {
            TrackingInfo track = tracked.get(id);
            try {
                if (track != null) {
                    boolean delayed = false;
                    if (idStreamSpec == null && type == TupleType.COORD || idStreamSpec != null && type == TupleType.ID) {
                        track.ackTuples.add(tup);
                        delayed = true;
                    }
                    if (track.failed) {
                        failed = true;
                        for (Tuple t : track.ackTuples) {
                            collector.fail(t);
                        }
                        tracked.remove(id);
                    } else if (track.receivedId && (sourceArgs.isEmpty()
                            || track.reportCount == numSourceReports && track.expectedTupleCount == track.receivedTuples)) {
                        if (delegate instanceof FinishedCallback) {
                            ((FinishedCallback) delegate).finishedId(id);
                        }
                        if (!(sourceArgs.isEmpty() || type != TupleType.REGULAR)) {
                            throw new IllegalStateException("Coordination condition met on a non-coordinating tuple. Should be impossible");
                        }
                        Iterator<Integer> outTasks = countOutTasks.iterator();
                        while (outTasks.hasNext()) {
                            int task = outTasks.next();
                            int numTuples = Utils.get(track.taskEmittedTuples, task, 0);
                            collector.emitDirect(task, Constants.COORDINATED_STREAM_ID, tup, new Values(id, numTuples));
                        }
                        for (Tuple t : track.ackTuples) {
                            collector.ack(t);
                        }
                        track.finished = true;
                        tracked.remove(id);
                    }
                    if (!delayed && type != TupleType.REGULAR) {
                        if (track.failed) {
                            collector.fail(tup);
                        } else {
                            collector.ack(tup);
                        }
                    }
                } else {
                    if (type != TupleType.REGULAR) {
                        collector.fail(tup);
                    }
                }
            } catch (FailedException e) {
                LOG.error("Failed to finish batch", e);
                for (Tuple t : track.ackTuples) {
                    collector.fail(t);
                }
                tracked.remove(id);
                failed = true;
            }
        }
        return failed;
    }

    @Override
    public void execute(Tuple tuple) {
        Object id = tuple.getValue(0);
        TrackingInfo track;
        TupleType type = getTupleType(tuple);
        synchronized (tracked) {
            track = tracked.get(id);
            if (track == null) {
                track = new TrackingInfo();
                if (idStreamSpec == null) {
                    track.receivedId = true;
                }
                tracked.put(id, track);
            }
        }

        if (type == TupleType.ID) {
            synchronized (tracked) {
                track.receivedId = true;
            }
            checkFinishId(tuple, type);
        } else if (type == TupleType.COORD) {
            int count = (Integer) tuple.getValue(1);
            synchronized (tracked) {
                track.reportCount++;
                track.expectedTupleCount += count;
            }
            checkFinishId(tuple, type);
        } else {
            synchronized (tracked) {
                delegate.execute(tuple);
            }
        }
    }

    @Override
    public void cleanup() {
        delegate.cleanup();
        tracked.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
        declarer.declareStream(Constants.COORDINATED_STREAM_ID, true, new Fields("id", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return delegate.getComponentConfiguration();
    }

    private TupleType getTupleType(Tuple tuple) {
        if (idStreamSpec != null
            && tuple.getSourceGlobalStreamId().equals(idStreamSpec.id)) {
            return TupleType.ID;
        } else if (!sourceArgs.isEmpty()
                   && tuple.getSourceStreamId().equals(Constants.COORDINATED_STREAM_ID)) {
            return TupleType.COORD;
        } else {
            return TupleType.REGULAR;
        }
    }

    enum TupleType {
        REGULAR,
        ID,
        COORD
    }

    public interface FinishedCallback {
        void finishedId(Object id);
    }

    public interface TimeoutCallback {
        void timeoutId(Object id);
    }

    public static class SourceArgs implements Serializable {
        public boolean singleCount;

        protected SourceArgs(boolean singleCount) {
            this.singleCount = singleCount;
        }

        public static SourceArgs single() {
            return new SourceArgs(true);
        }

        public static SourceArgs all() {
            return new SourceArgs(false);
        }

        @Override
        public String toString() {
            return "<Single: " + singleCount + ">";
        }
    }

    public static class TrackingInfo {
        int reportCount = 0;
        int expectedTupleCount = 0;
        int receivedTuples = 0;
        boolean failed = false;
        Map<Integer, Integer> taskEmittedTuples = new HashMap<>();
        boolean receivedId = false;
        boolean finished = false;
        List<Tuple> ackTuples = new ArrayList<>();

        @Override
        public String toString() {
            return "reportCount: " + reportCount + "\n"
                    + "expectedTupleCount: " + expectedTupleCount + "\n"
                    + "receivedTuples: " + receivedTuples + "\n"
                    + "failed: " + failed + "\n"
                    + taskEmittedTuples.toString();
        }
    }

    public static class IdStreamSpec implements Serializable {
        GlobalStreamId id;

        protected IdStreamSpec(String component, String stream) {
            id = new GlobalStreamId(component, stream);
        }

        public static IdStreamSpec makeDetectSpec(String component, String stream) {
            return new IdStreamSpec(component, stream);
        }

        public GlobalStreamId getGlobalStreamId() {
            return id;
        }
    }

    public class CoordinatedOutputCollector implements IOutputCollector {
        IOutputCollector delegate;

        public CoordinatedOutputCollector(IOutputCollector delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<Integer> emit(String stream, Collection<Tuple> anchors, List<Object> tuple) {
            List<Integer> tasks = delegate.emit(stream, anchors, tuple);
            updateTaskCounts(tuple.get(0), tasks);
            return tasks;
        }

        @Override
        public void emitDirect(int task, String stream, Collection<Tuple> anchors, List<Object> tuple) {
            updateTaskCounts(tuple.get(0), Arrays.asList(task));
            delegate.emitDirect(task, stream, anchors, tuple);
        }

        @Override
        public void ack(Tuple tuple) {
            Object id = tuple.getValue(0);
            synchronized (tracked) {
                TrackingInfo track = tracked.get(id);
                if (track != null) {
                    track.receivedTuples++;
                }
            }
            boolean failed = checkFinishId(tuple, TupleType.REGULAR);
            if (failed) {
                delegate.fail(tuple);
            } else {
                delegate.ack(tuple);
            }
        }

        @Override
        public void fail(Tuple tuple) {
            Object id = tuple.getValue(0);
            synchronized (tracked) {
                TrackingInfo track = tracked.get(id);
                if (track != null) {
                    track.failed = true;
                }
            }
            checkFinishId(tuple, TupleType.REGULAR);
            delegate.fail(tuple);
        }

        @Override
        public void flush() {
            delegate.flush();
        }

        @Override
        public void resetTimeout(Tuple tuple) {
            delegate.resetTimeout(tuple);
        }

        @Override
        public void reportError(Throwable error) {
            delegate.reportError(error);
        }


        private void updateTaskCounts(Object id, List<Integer> tasks) {
            synchronized (tracked) {
                TrackingInfo track = tracked.get(id);
                if (track != null) {
                    Map<Integer, Integer> taskEmittedTuples = track.taskEmittedTuples;
                    for (Integer task : tasks) {
                        int newCount = Utils.get(taskEmittedTuples, task, 0) + 1;
                        taskEmittedTuples.put(task, newCount);
                    }
                }
            }
        }
    }

    private class TimeoutItems implements TimeCacheMap.ExpiredCallback<Object, TrackingInfo> {
        @Override
        public void expire(Object id, TrackingInfo val) {
            synchronized (tracked) {
                // the combination of the lock and the finished flag ensure that
                // an id is never timed out if it has been finished
                val.failed = true;
                if (!val.finished) {
                    ((TimeoutCallback) delegate).timeoutId(id);
                }
            }
        }
    }
}
