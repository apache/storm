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
package org.apache.storm.windowing;

/**
 * An eviction policy that tracks count based on watermark ts and
 * evicts events upto the watermark based on a threshold count.
 *
 * @param <T> the type of event tracked by this policy.
 */
public class WatermarkCountEvictionPolicy<T> extends CountEvictionPolicy<T> {
    /*
     * The reference time in millis for window calculations and
     * expiring events. If not set it will default to System.currentTimeMillis()
     */
    private long referenceTime;
    private long processed = 0L;

    public WatermarkCountEvictionPolicy(int count) {
        super(count);
    }

    @Override
    public Action evict(Event<T> event) {
        Action action;
        if (event.getTimestamp() <= referenceTime && processed < currentCount.get()) {
            action = super.evict(event);
            if (action == Action.PROCESS) {
                ++processed;
            }
        } else {
            action = Action.KEEP;
        }
        return action;
    }

    @Override
    public void track(Event<T> event) {
        // NOOP
    }

    @Override
    public void setContext(EvictionContext context) {
        referenceTime = context.getReferenceTime();
        if (context.getCurrentCount() != null) {
            currentCount.set(context.getCurrentCount());
        } else {
            currentCount.set(processed + context.getSlidingCount());
        }
        processed = 0;
    }

    @Override
    public String toString() {
        return "WatermarkCountEvictionPolicy{" +
                "referenceTime=" + referenceTime +
                "} " + super.toString();
    }
}
