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

package org.apache.storm.utils;

import io.opentelemetry.context.Context;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TupleUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TupleUtils.class);

    private TupleUtils() {
        // No instantiation
    }

    public static boolean isTick(Tuple tuple) {
        return tuple != null
               && Constants.SYSTEM_COMPONENT_ID.equals(tuple.getSourceComponent())
               && Constants.SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId());
    }

    /**
     * Returns the OpenTelemetry context to run work for this tuple under, so that spans created
     * there join the tuple's trace, or {@link Context#root()} when the tuple carries none (see
     * {@link Config#TOPOLOGY_TRACING_ENABLED}). Never null. The context holds span ids only: it
     * parents new spans but gives no access to the execute span itself. Example:
     * {@code pool.submit(TupleUtils.traceContext(input).wrap(task))}.
     */
    public static Context traceContext(Tuple tuple) {
        Context context = tuple instanceof TupleImpl impl ? impl.getTraceContext() : null;
        return context != null ? context : Context.root();
    }

    public static <T> int chooseTaskIndex(List<T> keys, int numTasks) {
        return Math.floorMod(listHashCode(keys), numTasks);
    }

    private static <T> int listHashCode(List<T> alist) {
        if (alist == null) {
            return 1;
        } else {
            return Arrays.deepHashCode(alist.toArray());
        }
    }

    public static Map<String, Object> putTickFrequencyIntoComponentConfig(Map<String, Object> conf, int tickFreqSecs) {
        if (conf == null) {
            conf = new Config();
        }

        if (tickFreqSecs > 0) {
            LOG.info("Enabling tick tuple with interval [{}]", tickFreqSecs);
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFreqSecs);
        }

        return conf;
    }
}
