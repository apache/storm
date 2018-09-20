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

package org.apache.storm.executor.error;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.Config;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportError implements IReportError {

    private static final Logger LOG = LoggerFactory.getLogger(ReportError.class);

    private final Map<String, Object> topoConf;
    private final IStormClusterState stormClusterState;
    private final String stormId;
    private final String componentId;
    private final WorkerTopologyContext workerTopologyContext;

    private int maxPerInterval;
    private int errorIntervalSecs;
    private AtomicInteger intervalStartTime;
    private AtomicInteger intervalErrors;

    public ReportError(Map<String, Object> topoConf, IStormClusterState stormClusterState, String stormId, String componentId,
                       WorkerTopologyContext workerTopologyContext) {
        this.topoConf = topoConf;
        this.stormClusterState = stormClusterState;
        this.stormId = stormId;
        this.componentId = componentId;
        this.workerTopologyContext = workerTopologyContext;
        this.errorIntervalSecs = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS));
        this.maxPerInterval = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL));
        this.intervalStartTime = new AtomicInteger(Time.currentTimeSecs());
        this.intervalErrors = new AtomicInteger(0);
    }

    @Override
    public void report(Throwable error) {
        LOG.error("Error", error);
        if (Time.deltaSecs(intervalStartTime.get()) > errorIntervalSecs) {
            intervalErrors.set(0);
            intervalStartTime.set(Time.currentTimeSecs());
        }
        if (intervalErrors.incrementAndGet() <= maxPerInterval) {
            try {
                stormClusterState.reportError(stormId, componentId, Utils.hostname(),
                                              workerTopologyContext.getThisWorkerPort().longValue(), error);
            } catch (UnknownHostException e) {
                throw Utils.wrapInRuntime(e);
            }

        }
    }
}
