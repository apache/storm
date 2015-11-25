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
package com.alibaba.jstorm.task.backpressure;

import java.util.Map;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;

public abstract class Backpressure {
    private static final String BACKPRESSURE_DELAY_TIME = "topology.backpressure.delay.time";

    protected volatile boolean isBackpressureEnable;

    protected volatile double highWaterMark;
    protected volatile double lowWaterMark;

    protected volatile double triggerBpRatio;

    protected volatile long sleepTime;

    public Backpressure(Map stormConf) {
        this.isBackpressureEnable = ConfigExtension.isBackpressureEnable(stormConf);
        this.highWaterMark = ConfigExtension.getBackpressureWaterMarkHigh(stormConf);
        this.lowWaterMark = ConfigExtension.getBackpressureWaterMarkLow(stormConf);
        this.triggerBpRatio = ConfigExtension.getBackpressureCoordinatorRatio(stormConf);
    }

    protected void updateConfig(Map stormConf) {
        if (stormConf == null) {
            return;
        }

        if (stormConf.containsKey(ConfigExtension.TOPOLOGY_BACKPRESSURE_ENABLE)) {
            this.isBackpressureEnable = ConfigExtension.isBackpressureEnable(stormConf);
        }

        if (stormConf.containsKey(ConfigExtension.TOPOLOGY_BACKPRESSURE_WATER_MARK_HIGH)) {
            this.highWaterMark = ConfigExtension.getBackpressureWaterMarkHigh(stormConf);
        }

        if (stormConf.containsKey(ConfigExtension.TOPOLOGY_BACKPRESSURE_WATER_MARK_LOW)) {
            this.lowWaterMark = ConfigExtension.getBackpressureWaterMarkLow(stormConf);
        }

        if (stormConf.containsKey(ConfigExtension.TOPOLOGY_BACKPRESSURE_COORDINATOR_RATIO)) {
            this.triggerBpRatio = ConfigExtension.getBackpressureCoordinatorRatio(stormConf);
        }

        if (stormConf.containsKey(BACKPRESSURE_DELAY_TIME)) {
            long time = JStormUtils.parseLong(stormConf, 0l);
            if (time != 0l) {
                this.sleepTime = time;
            }
        }
    }

    public boolean isBackpressureConfigChange(Map stormConf) {
        if (stormConf == null) {
            return false;
        }

        if (stormConf.containsKey(ConfigExtension.TOPOLOGY_BACKPRESSURE_ENABLE) || 
                stormConf.containsKey(ConfigExtension.TOPOLOGY_BACKPRESSURE_WATER_MARK_HIGH) || 
                stormConf.containsKey(ConfigExtension.TOPOLOGY_BACKPRESSURE_WATER_MARK_LOW) || 
                stormConf.containsKey(ConfigExtension.TOPOLOGY_BACKPRESSURE_COORDINATOR_RATIO) ||
                stormConf.containsKey(BACKPRESSURE_DELAY_TIME)) {
            return true;
        } else {
            return false;
        }
    }
}
