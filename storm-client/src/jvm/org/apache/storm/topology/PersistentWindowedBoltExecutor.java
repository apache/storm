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

package org.apache.storm.topology;

import static org.apache.storm.windowing.persistence.WindowState.WindowPartition;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.storm.Config;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.State;
import org.apache.storm.state.StateFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.DefaultEvictionContext;
import org.apache.storm.windowing.EventImpl;
import org.apache.storm.windowing.WindowLifecycleListener;
import org.apache.storm.windowing.persistence.WindowState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a {@link IStatefulWindowedBolt} and handles the execution. Uses state and the underlying checkpointing mechanisms to save the
 * tuples in window to state. The tuples are also kept in-memory by transparently caching the window partitions and checkpointing them as
 * needed.
 */
public class PersistentWindowedBoltExecutor<T extends State> extends WindowedBoltExecutor implements IStatefulBolt<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentWindowedBoltExecutor.class);
    private final IStatefulWindowedBolt<T> statefulWindowedBolt;
    private transient OutputCollector outputCollector;
    private transient WindowState<Tuple> state;
    private transient boolean stateInitialized;
    private transient boolean prePrepared;
    private transient KeyValueState<String, Optional<?>> windowSystemState;

    public PersistentWindowedBoltExecutor(IStatefulWindowedBolt<T> bolt) {
        super(bolt);
        statefulWindowedBolt = bolt;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        List<String> registrations = (List<String>) topoConf.getOrDefault(Config.TOPOLOGY_STATE_KRYO_REGISTER, new ArrayList<>());
        registrations.add(ConcurrentLinkedQueue.class.getName());
        registrations.add(LinkedList.class.getName());
        registrations.add(AtomicInteger.class.getName());
        registrations.add(EventImpl.class.getName());
        registrations.add(WindowPartition.class.getName());
        registrations.add(DefaultEvictionContext.class.getName());
        topoConf.put(Config.TOPOLOGY_STATE_KRYO_REGISTER, registrations);
        prepare(topoConf, context, collector, getWindowState(topoConf, context), getPartitionState(topoConf, context),
                getWindowSystemState(topoConf, context));
    }

    // package access for unit tests
    void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector,
                 KeyValueState<Long, WindowPartition<Tuple>> windowState,
                 KeyValueState<String, Deque<Long>> partitionState,
                 KeyValueState<String, Optional<?>> windowSystemState) {
        outputCollector = collector;
        this.windowSystemState = windowSystemState;
        state = new WindowState<>(windowState, partitionState, windowSystemState, this::getState,
                                  statefulWindowedBolt.maxEventsInMemory());
        doPrepare(topoConf, context, new NoAckOutputCollector(collector), state, true);
        restoreWindowSystemState();
    }

    private void restoreWindowSystemState() {
        Map<String, Optional<?>> map = new HashMap<>();
        for (Map.Entry<String, Optional<?>> entry : windowSystemState) {
            map.put(entry.getKey(), entry.getValue());
        }
        restoreState(map);
    }

    @Override
    protected void validate(Map<String, Object> topoConf,
                            BaseWindowedBolt.Count windowLengthCount,
                            BaseWindowedBolt.Duration windowLengthDuration,
                            BaseWindowedBolt.Count slidingIntervalCount,
                            BaseWindowedBolt.Duration slidingIntervalDuration) {
        if (windowLengthCount == null && windowLengthDuration == null) {
            throw new IllegalArgumentException("Window length is not specified");
        }
        int interval = getCheckpointIntervalMillis(topoConf);
        int timeout = getTopologyTimeoutMillis(topoConf);
        if (interval > timeout) {
            throw new IllegalArgumentException(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL + interval
                                               + " is more than " + Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS
                                               + " value " + timeout);
        }
    }

    private int getCheckpointIntervalMillis(Map<String, Object> topoConf) {
        int checkpointInterval = Integer.MAX_VALUE;
        if (topoConf.get(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL) != null) {
            checkpointInterval = ((Number) topoConf.get(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)).intValue();
        }
        return checkpointInterval;
    }

    @Override
    protected void start() {
        if (stateInitialized) {
            super.start();
        } else {
            LOG.debug("Will invoke start after state is initialized");
        }
    }

    @Override
    public void execute(Tuple input) {
        if (!stateInitialized) {
            throw new IllegalStateException("execute invoked before initState with input tuple " + input);
        }
        super.execute(input);
        // StatefulBoltExecutor does the actual ack when the state is saved.
        outputCollector.ack(input);
    }

    @Override
    public void initState(T state) {
        if (stateInitialized) {
            String msg = "initState invoked when the state is already initialized";
            LOG.warn(msg);
            throw new IllegalStateException(msg);
        } else {
            statefulWindowedBolt.initState(state);
            stateInitialized = true;
            start();
        }
    }

    @Override
    public void prePrepare(long txid) {
        if (stateInitialized) {
            LOG.debug("Prepare streamState, txid {}", txid);
            statefulWindowedBolt.prePrepare(txid);
            state.prepareCommit(txid);
            prePrepared = true;
        } else {
            String msg = "Cannot prepare before initState";
            LOG.warn(msg);
            throw new IllegalStateException(msg);
        }
    }

    @Override
    public void preCommit(long txid) {
        // preCommit can be invoked during recovery before the state is initialized
        if (prePrepared || !stateInitialized) {
            LOG.debug("Commit streamState, txid {}", txid);
            statefulWindowedBolt.preCommit(txid);
            state.commit(txid);
        } else {
            String msg = "preCommit before prePrepare in initialized state";
            LOG.warn(msg);
            throw new IllegalStateException(msg);
        }
    }

    @Override
    public void preRollback() {
        LOG.debug("Rollback streamState, stateInitialized {}", stateInitialized);
        statefulWindowedBolt.preRollback();
        state.rollback(stateInitialized);
        if (stateInitialized) {
            restoreWindowSystemState();
        }
    }

    @Override
    protected WindowLifecycleListener<Tuple> newWindowLifecycleListener() {
        return new WindowLifecycleListener<Tuple>() {
            @Override
            public void onExpiry(List<Tuple> events) {
                /*
                 * NO-OP: the events are ack-ed in execute
                 */
            }

            @Override
            public void onActivation(Supplier<Iterator<Tuple>> eventsIt,
                                     Supplier<Iterator<Tuple>> newEventsIt,
                                     Supplier<Iterator<Tuple>> expiredIt,
                                     Long timestamp) {
                /*
                 * Here we don't set the tuples in windowedOutputCollector's context and emit un-anchored.
                 * The checkpoint tuple will trigger a checkpoint in the receiver with the emitted tuples.
                 */
                boltExecute(eventsIt, newEventsIt, expiredIt, timestamp);
                state.clearIteratorPins();
            }
        };
    }

    private KeyValueState<Long, WindowPartition<Tuple>> getWindowState(Map<String, Object> topoConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window";
        return (KeyValueState<Long, WindowPartition<Tuple>>) StateFactory.getState(namespace, topoConf, context);
    }

    private KeyValueState<String, Deque<Long>> getPartitionState(Map<String, Object> topoConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window-partitions";
        return (KeyValueState<String, Deque<Long>>) StateFactory.getState(namespace, topoConf, context);
    }

    private KeyValueState<String, Optional<?>> getWindowSystemState(Map<String, Object> topoConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window-systemstate";
        return (KeyValueState<String, Optional<?>>) StateFactory.getState(namespace, topoConf, context);
    }

    /**
     * Creates an {@link OutputCollector} wrapper that ignores acks. The {@link PersistentWindowedBoltExecutor} acks the tuples in execute
     * and this is to prevent double ack-ing
     */
    private static class NoAckOutputCollector extends OutputCollector {

        NoAckOutputCollector(OutputCollector delegate) {
            super(delegate);
        }

        @Override
        public void ack(Tuple input) {
            // NOOP
        }
    }
}
