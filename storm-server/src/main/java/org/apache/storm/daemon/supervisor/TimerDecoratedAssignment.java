/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.storm.daemon.supervisor;

import com.codahale.metrics.Timer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.metric.timed.TimerDecorated;

public class TimerDecoratedAssignment extends LocalAssignment implements TimerDecorated {
    //TODO: Does this have to volatile?
    private final AtomicReference<Timer.Context> timingRef;

    public TimerDecoratedAssignment(LocalAssignment other, Timer timer) {
        super(other);
        timingRef = new AtomicReference<>(timer.time());
    }

    @Override
    public boolean hasStopped() {
        return hasStopped(timingRef);
    }

    @Override
    public long stopTiming() {
        return stopTiming(timingRef);
    }
}
