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

package org.apache.storm.messaging;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.esotericsoftware.kryo.KryoException;
import org.apache.storm.Config;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.serialization.KryoTupleDeserializer;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that is called when a TaskMessage arrives.
 */
public class DeserializingConnectionCallback implements IConnectionCallback, IMetric {
    private static final Logger LOG = LoggerFactory.getLogger(DeserializingConnectionCallback.class);

    // A tuple that cannot be decoded is dropped instead of killing the worker; anything outside this set keeps
    // the fatal handling in StormServerHandler.
    private static final Set<Class<?>> TOLERATED_DESERIALIZATION_FAILURES = new HashSet<>(Arrays.asList(
        IOException.class,
        KryoException.class,
        IllegalArgumentException.class,
        NegativeArraySizeException.class,
        ClassCastException.class,
        ArrayIndexOutOfBoundsException.class,
        BufferUnderflowException.class,
        NullPointerException.class,
        ClassNotFoundException.class));

    static final String DESERIALIZATION_FAILURES_KEY = "deserializationFailures";

    private final WorkerState.ILocalTransferCallback cb;
    private final Map<String, Object> conf;
    private final GeneralTopologyContext context;

    private final ThreadLocal<KryoTupleDeserializer> des =
        new ThreadLocal<KryoTupleDeserializer>() {
            @Override
            protected KryoTupleDeserializer initialValue() {
                return new KryoTupleDeserializer(conf, context);
            }
        };

    // Track serialized size of messages.
    private final boolean sizeMetricsEnabled;
    private final ConcurrentHashMap<String, AtomicLong> byteCounts = new ConcurrentHashMap<>();
    private final AtomicLong deserializationFailures = new AtomicLong(0L);


    public DeserializingConnectionCallback(final Map<String, Object> conf, final GeneralTopologyContext context,
                                           WorkerState.ILocalTransferCallback callback) {
        this.conf = conf;
        this.context = context;
        cb = callback;
        sizeMetricsEnabled = ObjectReader.getBoolean(conf.get(Config.TOPOLOGY_SERIALIZED_MESSAGE_SIZE_METRICS), false);

    }

    @Override
    public void recv(List<TaskMessage> batch) {
        KryoTupleDeserializer des = this.des.get();
        ArrayList<AddressedTuple> ret = new ArrayList<>(batch.size());
        for (TaskMessage message : batch) {
            try {
                Tuple tuple = des.deserialize(message.message());
                AddressedTuple addrTuple = new AddressedTuple(message.task(), tuple);
                updateMetrics(tuple.getSourceTask(), message);
                ret.add(addrTuple);
            } catch (Exception e) {
                if (!isToleratedDeserializationFailure(e)) {
                    throw e;
                }
                deserializationFailures.incrementAndGet();
                LOG.error("Failed to deserialize a message of {} bytes destined for task {}, dropping it",
                          message.message().length, message.task(), e);
            }
        }
        cb.transfer(ret);
    }

    private static boolean isToleratedDeserializationFailure(Exception e) {
        for (Class<?> klass : TOLERATED_DESERIALIZATION_FAILURES) {
            if (Utils.exceptionCauseIsInstanceOf(klass, e)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns serialized byte count traffic metrics and the count of dropped deserialization failures.
     *
     * @return Map of metric counts, or null when size metrics are disabled and no failures occurred
     */
    @Override
    public Object getValueAndReset() {
        long failures = deserializationFailures.getAndSet(0L);
        if (!sizeMetricsEnabled) {
            return failures > 0 ? Collections.singletonMap(DESERIALIZATION_FAILURES_KEY, failures) : null;
        }
        HashMap<String, Long> outMap = new HashMap<>();
        for (Map.Entry<String, AtomicLong> ent : byteCounts.entrySet()) {
            AtomicLong count = ent.getValue();
            if (count.get() > 0) {
                outMap.put(ent.getKey(), count.getAndSet(0L));
            }
        }
        if (failures > 0) {
            outMap.put(DESERIALIZATION_FAILURES_KEY, failures);
        }
        return outMap;
    }

    /**
     * Update serialized byte counts for each message.
     *
     * @param sourceTaskId source task
     * @param message      serialized message
     */
    protected void updateMetrics(int sourceTaskId, TaskMessage message) {
        if (sizeMetricsEnabled) {
            int dest = message.task();
            int len = message.message().length;
            String key = Integer.toString(sourceTaskId) + "-" + Integer.toString(dest);
            byteCounts.computeIfAbsent(key, k -> new AtomicLong(0L)).addAndGet(len);
        }
    }

}
