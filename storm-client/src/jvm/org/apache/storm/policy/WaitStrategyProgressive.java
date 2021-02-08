/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.policy;

import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import org.apache.storm.Config;
import org.apache.storm.utils.ObjectReader;

/**
 * A Progressive Wait Strategy.
 *
 * <p>Has three levels of idling. Stays in each level for a configured number of iterations before entering the next level.
 * Level 1 - No idling. Returns immediately. Stays in this level for `level1Count` iterations. Level 2 - Calls LockSupport.parkNanos(1).
 * Stays in this level for `level2Count` iterations Level 3 - Calls Thread.sleep(). Stays in this level until wait situation changes.
 *
 * <p>The initial spin can be useful to prevent downstream bolt from repeatedly sleeping/parking when the upstream component is a bit
 * relatively slower. Allows downstream bolt can enter deeper wait states only if the traffic to it appears to have reduced.
 */
public class WaitStrategyProgressive implements IWaitStrategy {
    private int level1Count;
    private int level2Count;
    private long level3SleepMs;

    @Override
    public void prepare(Map<String, Object> conf, WaitSituation waitSituation) {
        if (waitSituation == WaitSituation.SPOUT_WAIT) {
            level1Count = ObjectReader.getInt(conf.get(Config.TOPOLOGY_SPOUT_WAIT_PROGRESSIVE_LEVEL1_COUNT));
            level2Count = ObjectReader.getInt(conf.get(Config.TOPOLOGY_SPOUT_WAIT_PROGRESSIVE_LEVEL2_COUNT));
            level3SleepMs = ObjectReader.getLong(conf.get(Config.TOPOLOGY_SPOUT_WAIT_PROGRESSIVE_LEVEL3_SLEEP_MILLIS));
        } else if (waitSituation == WaitSituation.BOLT_WAIT) {
            level1Count = ObjectReader.getInt(conf.get(Config.TOPOLOGY_BOLT_WAIT_PROGRESSIVE_LEVEL1_COUNT));
            level2Count = ObjectReader.getInt(conf.get(Config.TOPOLOGY_BOLT_WAIT_PROGRESSIVE_LEVEL2_COUNT));
            level3SleepMs = ObjectReader.getLong(conf.get(Config.TOPOLOGY_BOLT_WAIT_PROGRESSIVE_LEVEL3_SLEEP_MILLIS));
        } else if (waitSituation == WaitSituation.BACK_PRESSURE_WAIT) {
            level1Count = ObjectReader.getInt(conf.get(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL1_COUNT));
            level2Count = ObjectReader.getInt(conf.get(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL2_COUNT));
            level3SleepMs = ObjectReader.getLong(conf.get(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL3_SLEEP_MILLIS));
        } else {
            throw new IllegalArgumentException("Unknown wait situation : " + waitSituation);
        }
    }

    @Override
    public int idle(int idleCounter) throws InterruptedException {
        if (idleCounter < level1Count) {                     // level 1 - no waiting
            ++idleCounter;
        } else if (idleCounter < level1Count + level2Count) { // level 2 - parkNanos(1L)
            ++idleCounter;
            LockSupport.parkNanos(1L);
        } else {                                      // level 3 - longer idling with Thread.sleep()
            Thread.sleep(level3SleepMs);
        }
        return idleCounter;
    }
}
