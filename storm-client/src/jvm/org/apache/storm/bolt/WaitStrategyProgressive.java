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

package org.apache.storm.bolt;

import org.apache.storm.Config;
import org.apache.storm.utils.ObjectReader;

import java.util.Map;
import java.util.concurrent.locks.LockSupport;

/**
 * Initially spins, then downgrades to LockSupport.parkNanos(), and eventually Thread.sleep()
 * Provides control over how to progress to the next wait level.
 * Larger the 'step' and 'multiplier', slower the progression.
 * Latency increases with every progression to the next wait level.
 */
public class WaitStrategyProgressive implements IBoltWaitStrategy {
    private long sleepMillis;
    private int step;
    private int multiplier;

    @Override
    public void prepare(Map<String, Object> conf) {
        sleepMillis = ObjectReader.getLong(conf.get(Config.TOPOLOGY_BOLT_WAIT_STRATEGY_PROGRESSIVE_MILLIS));
        step = ObjectReader.getInt(conf.get(Config.TOPOLOGY_BOLT_WAIT_STRATEGY_PROGRESSIVE_STEP));
        multiplier = ObjectReader.getInt(conf.get(Config.TOPOLOGY_BOLT_WAIT_STRATEGY_PROGRESSIVE_MULTIPLIER));
    }

    @Override
    public int idle(int idleCounter) throws InterruptedException {
        if (idleCounter < step) {
            ++idleCounter;
        } else if (idleCounter < step * multiplier) {
            ++idleCounter;
            LockSupport.parkNanos(1L);
        } else {
            Thread.sleep(sleepMillis);
        }
        return idleCounter;
    }
}
