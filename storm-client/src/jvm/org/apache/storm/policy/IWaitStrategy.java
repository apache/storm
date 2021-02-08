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
import org.apache.storm.Config;
import org.apache.storm.utils.ReflectionUtils;


public interface IWaitStrategy {
    static IWaitStrategy createBackPressureWaitStrategy(Map<String, Object> topologyConf) {
        IWaitStrategy producerWaitStrategy =
            ReflectionUtils.newInstance((String) topologyConf.get(Config.TOPOLOGY_BACKPRESSURE_WAIT_STRATEGY));
        producerWaitStrategy.prepare(topologyConf, WaitSituation.BACK_PRESSURE_WAIT);
        return producerWaitStrategy;
    }

    void prepare(Map<String, Object> conf, WaitSituation waitSituation);

    /**
     * Implementations of this method should be thread-safe (preferably no side-effects and lock-free).
     *
     * <p>Supports static or dynamic backoff. Dynamic backoff relies on idleCounter to estimate how long caller has been idling.
     * <pre>
     * <code>
     *  int idleCounter = 0;
     *  int consumeCount = consumeFromQ();
     *  while (consumeCount==0) {
     *     idleCounter = strategy.idle(idleCounter);
     *     consumeCount = consumeFromQ();
     *  }
     * </code>
     * </pre>
     *
     * @param idleCounter managed by the idle method until reset
     * @return new counter value to be used on subsequent idle cycle
     */
    int idle(int idleCounter) throws InterruptedException;

    enum WaitSituation {
        SPOUT_WAIT,
        BOLT_WAIT,
        BACK_PRESSURE_WAIT
    }
}
