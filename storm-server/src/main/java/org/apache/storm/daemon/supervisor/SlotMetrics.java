/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.supervisor;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import java.util.Collections;
import java.util.Map;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.EnumUtil;

class SlotMetrics {

    final Meter numWorkersLaunched;
    final Meter numWorkerStartTimedOut;
    final Map<Slot.KillReason, Meter> numWorkersKilledFor;
    final Timer workerLaunchDuration;
    final Map<Slot.MachineState, Meter> transitionIntoState;
    //This also tracks how many times worker transitioning out of a state
    final Map<Slot.MachineState, Timer> timeSpentInState;

    SlotMetrics(StormMetricsRegistry metricsRegistry) {
        numWorkersLaunched = metricsRegistry.registerMeter("supervisor:num-workers-launched");
        numWorkerStartTimedOut = metricsRegistry.registerMeter("supervisor:num-worker-start-timed-out");
        numWorkersKilledFor = Collections.unmodifiableMap(EnumUtil.toEnumMap(Slot.KillReason.class,
            killReason -> metricsRegistry.registerMeter("supervisor:num-workers-killed-" + killReason.toString())));
        workerLaunchDuration = metricsRegistry.registerTimer("supervisor:worker-launch-duration");
        transitionIntoState = Collections.unmodifiableMap(EnumUtil.toEnumMap(Slot.MachineState.class,
            machineState -> metricsRegistry.registerMeter("supervisor:num-worker-transitions-into-" + machineState.toString())));
        timeSpentInState = Collections.unmodifiableMap(EnumUtil.toEnumMap(Slot.MachineState.class,
            machineState -> metricsRegistry.registerTimer("supervisor:time-worker-spent-in-state-" + machineState.toString() + "-ms")));
    }

}
