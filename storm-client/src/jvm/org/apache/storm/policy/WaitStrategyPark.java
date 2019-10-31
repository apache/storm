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

public class WaitStrategyPark implements IWaitStrategy {
    private long parkTimeNanoSec;

    public WaitStrategyPark() { // required for instantiation via reflection. must call prepare() thereafter
    }

    // Convenience alternative to prepare() for use in Tests
    public WaitStrategyPark(long microsec) {
        parkTimeNanoSec = microsec * 1_000;
    }

    @Override
    public void prepare(Map<String, Object> conf, WaitSituation waitSituation) {
        if (waitSituation == WaitSituation.SPOUT_WAIT) {
            parkTimeNanoSec = 1_000 * ObjectReader.getLong(conf.get(Config.TOPOLOGY_SPOUT_WAIT_PARK_MICROSEC));
        } else if (waitSituation == WaitSituation.BOLT_WAIT) {
            parkTimeNanoSec = 1_000 * ObjectReader.getLong(conf.get(Config.TOPOLOGY_BOLT_WAIT_PARK_MICROSEC));
        } else if (waitSituation == WaitSituation.BACK_PRESSURE_WAIT) {
            parkTimeNanoSec = 1_000 * ObjectReader.getLong(conf.get(Config.TOPOLOGY_BACKPRESSURE_WAIT_PARK_MICROSEC));
        } else {
            throw new IllegalArgumentException("Unknown wait situation : " + waitSituation);
        }
    }

    @Override
    public int idle(int idleCounter) throws InterruptedException {
        if (parkTimeNanoSec == 0) {
            return 1;
        }
        LockSupport.parkNanos(parkTimeNanoSec);
        return idleCounter + 1;
    }
}
