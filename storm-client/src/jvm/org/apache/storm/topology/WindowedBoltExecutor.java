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
package org.apache.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.spout.CheckpointSpout;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.CountEvictionPolicy;
import org.apache.storm.windowing.CountTriggerPolicy;
import org.apache.storm.windowing.Event;
import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.StatefulWindowManager;
import org.apache.storm.windowing.TimeEvictionPolicy;
import org.apache.storm.windowing.TimeTriggerPolicy;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.TriggerPolicy;
import org.apache.storm.windowing.TupleWindowImpl;
import org.apache.storm.windowing.TupleWindowIterImpl;
import org.apache.storm.windowing.WaterMarkEventGenerator;
import org.apache.storm.windowing.WatermarkCountEvictionPolicy;
import org.apache.storm.windowing.WatermarkCountTriggerPolicy;
import org.apache.storm.windowing.WatermarkTimeEvictionPolicy;
import org.apache.storm.windowing.WatermarkTimeTriggerPolicy;
import org.apache.storm.windowing.WindowLifecycleListener;
import org.apache.storm.windowing.WindowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.storm.topology.base.BaseWindowedBolt.Count;
import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * An {@link IWindowedBolt} wrapper that does the windowing of tuples.
 */
public class WindowedBoltExecutor implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WindowedBoltExecutor.class);
    private static final int DEFAULT_WATERMARK_EVENT_INTERVAL_MS = 1000; // 1s
    private static final int DEFAULT_MAX_LAG_MS = 0; // no lag
    public static final String LATE_TUPLE_FIELD = "late_tuple";
    private final IWindowedBolt bolt;
    private transient WindowedOutputCollector windowedOutputCollector;
    private transient WindowLifecycleListener<Tuple> listener;
    private transient WindowManager<Tuple> windowManager;
    private transient int maxLagMs;
    private TimestampExtractor timestampExtractor;
    private transient String lateTupleStream;
    private transient TriggerPolicy<Tuple, ?> triggerPolicy;
    private transient EvictionPolicy<Tuple,?> evictionPolicy;
    private transient Duration windowLengthDuration;
    // package level for unit tests
    transient WaterMarkEventGenerator<Tuple> waterMarkEventGenerator;

    public WindowedBoltExecutor(IWindowedBolt bolt) {
        this.bolt = bolt;
        timestampExtractor = bolt.getTimestampExtractor();
    }

    protected int getTopologyTimeoutMillis(Map<String, Object> topoConf) {
        if (topoConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS) != null) {
            boolean timeOutsEnabled = (boolean) topoConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS);
            if (!timeOutsEnabled) {
                return Integer.MAX_VALUE;
            }
        }
        int timeout = 0;
        if (topoConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS) != null) {
            timeout = ((Number) topoConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
        }
        return timeout * 1000;
    }

    private int getMaxSpoutPending(Map<String, Object> topoConf) {
        int maxPending = Integer.MAX_VALUE;
        if (topoConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING) != null) {
            maxPending = ((Number) topoConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING)).intValue();
        }
        return maxPending;
    }

    private void ensureDurationLessThanTimeout(int duration, int timeout) {
        if (duration > timeout) {
            throw new IllegalArgumentException("Window duration (length + sliding interval) value " + duration +
                                                       " is more than " + Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS +
                                                       " value " + timeout);
        }
    }

    private void ensureCountLessThanMaxPending(int count, int maxPending) {
        if (count > maxPending) {
            throw new IllegalArgumentException("Window count (length + sliding interval) value " + count +
                                                       " is more than " + Config.TOPOLOGY_MAX_SPOUT_PENDING +
                                                       " value " + maxPending);
        }
    }

    protected void validate(Map<String, Object> topoConf, Count windowLengthCount, Duration windowLengthDuration,
                            Count slidingIntervalCount, Duration slidingIntervalDuration) {

        int topologyTimeout = getTopologyTimeoutMillis(topoConf);
        int maxSpoutPending = getMaxSpoutPending(topoConf);
        if (windowLengthCount == null && windowLengthDuration == null) {
            throw new IllegalArgumentException("Window length is not specified");
        }

        if (windowLengthDuration != null && slidingIntervalDuration != null) {
            ensureDurationLessThanTimeout(windowLengthDuration.value + slidingIntervalDuration.value, topologyTimeout);
        } else if (windowLengthDuration != null) {
            ensureDurationLessThanTimeout(windowLengthDuration.value, topologyTimeout);
        } else if (slidingIntervalDuration != null) {
            ensureDurationLessThanTimeout(slidingIntervalDuration.value, topologyTimeout);
        }

        if (windowLengthCount != null && slidingIntervalCount != null) {
            ensureCountLessThanMaxPending(windowLengthCount.value + slidingIntervalCount.value, maxSpoutPending);
        } else if (windowLengthCount != null) {
            ensureCountLessThanMaxPending(windowLengthCount.value, maxSpoutPending);
        } else if (slidingIntervalCount != null) {
            ensureCountLessThanMaxPending(slidingIntervalCount.value, maxSpoutPending);
        }
    }

    private WindowManager<Tuple> initWindowManager(WindowLifecycleListener<Tuple> lifecycleListener, Map<String, Object> topoConf,
                                                   TopologyContext context, Collection<Event<Tuple>> queue, boolean stateful) {

        WindowManager<Tuple> manager = stateful ?
            new StatefulWindowManager<>(lifecycleListener, queue)
            : new WindowManager<>(lifecycleListener, queue);

        Count windowLengthCount = null;
        Duration slidingIntervalDuration = null;
        Count slidingIntervalCount = null;
        // window length
        if (topoConf.containsKey(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)) {
            windowLengthCount = new Count(((Number) topoConf.get(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)).intValue());
        } else if (topoConf.containsKey(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)) {
            windowLengthDuration = new Duration(
                    ((Number) topoConf.get(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)).intValue(),
                    TimeUnit.MILLISECONDS);
        }
        // sliding interval
        if (topoConf.containsKey(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT)) {
            slidingIntervalCount = new Count(((Number) topoConf.get(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT)).intValue());
        } else if (topoConf.containsKey(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS)) {
            slidingIntervalDuration = new Duration(((Number) topoConf.get(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS)).intValue(), TimeUnit.MILLISECONDS);
        } else {
            // default is a sliding window of count 1
            slidingIntervalCount = new Count(1);
        }
        // tuple ts
        if (timestampExtractor != null) {
            // late tuple stream
            lateTupleStream = (String) topoConf.get(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
            if (lateTupleStream != null) {
                if (!context.getThisStreams().contains(lateTupleStream)) {
                    throw new IllegalArgumentException("Stream for late tuples must be defined with the builder method withLateTupleStream");
                }
            }
            // max lag
            if (topoConf.containsKey(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS)) {
                maxLagMs = ((Number) topoConf.get(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS)).intValue();
            } else {
                maxLagMs = DEFAULT_MAX_LAG_MS;
            }
            // watermark interval
            int watermarkInterval;
            if (topoConf.containsKey(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS)) {
                watermarkInterval = ((Number) topoConf.get(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS)).intValue();
            } else {
                watermarkInterval = DEFAULT_WATERMARK_EVENT_INTERVAL_MS;
            }
            waterMarkEventGenerator = new WaterMarkEventGenerator<>(manager, watermarkInterval,
                                                                    maxLagMs, getComponentStreams(context));
        } else {
            if (topoConf.containsKey(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM)) {
                throw new IllegalArgumentException("Late tuple stream can be defined only when specifying a timestamp field");
            }
        }
        // validate
        validate(topoConf, windowLengthCount, windowLengthDuration,
                 slidingIntervalCount, slidingIntervalDuration);
        evictionPolicy = getEvictionPolicy(windowLengthCount, windowLengthDuration);
        triggerPolicy = getTriggerPolicy(slidingIntervalCount, slidingIntervalDuration,
                                                              manager, evictionPolicy);
        manager.setEvictionPolicy(evictionPolicy);
        manager.setTriggerPolicy(triggerPolicy);
        return manager;
    }

    protected void restoreState(Map<String, Optional<?>> state) {
        windowManager.restoreState(state);
    }

    protected Map<String, Optional<?>> getState() {
        return windowManager.getState();
    }

    private Set<GlobalStreamId> getComponentStreams(TopologyContext context) {
        Set<GlobalStreamId> streams = new HashSet<>();
        for (GlobalStreamId streamId : context.getThisSources().keySet()) {
            if (!streamId.get_streamId().equals(CheckpointSpout.CHECKPOINT_STREAM_ID)) {
                streams.add(streamId);
            }
        }
        return streams;
    }

    /**
     * Start the trigger policy and waterMarkEventGenerator if set
     */
    protected void start() {
        if (waterMarkEventGenerator != null) {
            LOG.debug("Starting waterMarkEventGenerator");
            waterMarkEventGenerator.start();
        }
        LOG.debug("Starting trigger policy");
        triggerPolicy.start();
    }

    private boolean isTupleTs() {
        return timestampExtractor != null;
    }

    private TriggerPolicy<Tuple, ?> getTriggerPolicy(Count slidingIntervalCount, Duration slidingIntervalDuration,
                                                  WindowManager<Tuple> manager, EvictionPolicy<Tuple, ?> evictionPolicy) {
        if (slidingIntervalCount != null) {
            if (isTupleTs()) {
                return new WatermarkCountTriggerPolicy<>(slidingIntervalCount.value, manager, evictionPolicy, manager);
            } else {
                return new CountTriggerPolicy<>(slidingIntervalCount.value, manager, evictionPolicy);
            }
        } else {
            if (isTupleTs()) {
                return new WatermarkTimeTriggerPolicy<>(slidingIntervalDuration.value, manager, evictionPolicy, manager);
            } else {
                return new TimeTriggerPolicy<>(slidingIntervalDuration.value, manager, evictionPolicy);
            }
        }
    }

    private EvictionPolicy<Tuple, ?> getEvictionPolicy(Count windowLengthCount, Duration windowLengthDuration) {
        if (windowLengthCount != null) {
            if (isTupleTs()) {
                return new WatermarkCountEvictionPolicy<>(windowLengthCount.value);
            } else {
                return new CountEvictionPolicy<>(windowLengthCount.value);
            }
        } else {
            if (isTupleTs()) {
                return new WatermarkTimeEvictionPolicy<>(windowLengthDuration.value, maxLagMs);
            } else {
                return new TimeEvictionPolicy<>(windowLengthDuration.value);
            }
        }
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        doPrepare(topoConf, context, collector, new ConcurrentLinkedQueue<>(), false);
    }

    // NOTE: the queue has to be thread safe.
    protected void doPrepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector,
                             Collection<Event<Tuple>> queue, boolean stateful) {
        Objects.requireNonNull(topoConf);
        Objects.requireNonNull(context);
        Objects.requireNonNull(collector);
        Objects.requireNonNull(queue);
        this.windowedOutputCollector = new WindowedOutputCollector(collector);
        bolt.prepare(topoConf, context, windowedOutputCollector);
        this.listener = newWindowLifecycleListener();
        this.windowManager = initWindowManager(listener, topoConf, context, queue, stateful);
        start();
        LOG.info("Initialized window manager {} ", windowManager);
    }

    @Override
    public void execute(Tuple input) {
        if (isTupleTs()) {
            long ts = timestampExtractor.extractTimestamp(input);
            if (waterMarkEventGenerator.track(input.getSourceGlobalStreamId(), ts)) {
                windowManager.add(input, ts);
            } else {
                if (lateTupleStream != null) {
                    windowedOutputCollector.emit(lateTupleStream, input, new Values(input));
                } else {
                    LOG.info("Received a late tuple {} with ts {}. This will not be processed.", input, ts);
                }
                windowedOutputCollector.ack(input);
            }
        } else {
            windowManager.add(input);
        }
    }

    @Override
    public void cleanup() {
        waterMarkEventGenerator.shutdown();
        windowManager.shutdown();
        bolt.cleanup();
    }

    // for unit tests
    WindowManager<Tuple> getWindowManager() {
        return windowManager;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        String lateTupleStream = (String) getComponentConfiguration().get(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
        if (lateTupleStream != null) {
            declarer.declareStream(lateTupleStream, new Fields(LATE_TUPLE_FIELD));
        }
        bolt.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return bolt.getComponentConfiguration();
    }

    protected WindowLifecycleListener<Tuple> newWindowLifecycleListener() {
        return new WindowLifecycleListener<Tuple>() {
            @Override
            public void onExpiry(List<Tuple> tuples) {
                for (Tuple tuple : tuples) {
                    windowedOutputCollector.ack(tuple);
                }
            }

            @Override
            public void onActivation(List<Tuple> tuples, List<Tuple> newTuples, List<Tuple> expiredTuples, Long timestamp) {
                windowedOutputCollector.setContext(tuples);
                boltExecute(tuples, newTuples, expiredTuples, timestamp);
            }

        };
    }

    protected void boltExecute(List<Tuple> tuples, List<Tuple> newTuples, List<Tuple> expiredTuples, Long timestamp) {
        bolt.execute(new TupleWindowImpl(tuples, newTuples, expiredTuples, getWindowStartTs(timestamp), timestamp));
    }

    protected void boltExecute(Supplier<Iterator<Tuple>> tuples,
                               Supplier<Iterator<Tuple>> newTuples,
                               Supplier<Iterator<Tuple>> expiredTuples, Long timestamp) {
        bolt.execute(new TupleWindowIterImpl(tuples, newTuples, expiredTuples, getWindowStartTs(timestamp), timestamp));
    }

    private Long getWindowStartTs(Long endTs) {
        Long res = null;
        if (endTs != null && windowLengthDuration != null) {
            res = endTs - windowLengthDuration.value;
        }
        return res;
    }

    /**
     * Creates an {@link OutputCollector} wrapper that automatically
     * anchors the tuples to inputTuples while emitting.
     */
    private static class WindowedOutputCollector extends OutputCollector {
        private List<Tuple> inputTuples;

        WindowedOutputCollector(IOutputCollector delegate) {
            super(delegate);
        }

        void setContext(List<Tuple> inputTuples) {
            this.inputTuples = inputTuples;
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple) {
            return emit(streamId, inputTuples, tuple);
        }

        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple) {
            emitDirect(taskId, streamId, inputTuples, tuple);
        }
    }

}
