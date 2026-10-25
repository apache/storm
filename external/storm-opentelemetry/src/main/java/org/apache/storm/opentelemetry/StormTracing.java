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

package org.apache.storm.opentelemetry;

import io.opentelemetry.context.Context;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;

/**
 * Gives bolts the trace context of the tuples they process, when {@link OpenTelemetryTupleTracer} traces the topology.
 */
public final class StormTracing {

    private StormTracing() {
    }

    /**
     * Returns the context to run work for this tuple under, so that spans created there join the tuple's trace, or
     * {@link Context#root()} when the tuple carries none. Never null. The context holds span ids only: it parents new spans but
     * gives no access to the execute span itself. Example: {@code pool.submit(StormTracing.context(input).wrap(task))}.
     */
    public static Context context(Tuple tuple) {
        return tuple instanceof TupleImpl impl && impl.getTraceContext() instanceof Context context ? context : Context.root();
    }
}
