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

package org.apache.storm.testing;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.Testing;

/**
 * The param class for the `Testing.completeTopology`.
 */
public class CompleteTopologyParam {
    /**
     * The mocked spout sources.
     */
    private MockedSources mockedSources = new MockedSources();
    /**
     * the config for the topology when it was submitted to the cluster.
     */
    private Map<String, Object> topoConf = new Config();
    /**
     * Indicates whether to cleanup the state.
     */
    private boolean cleanupState = true;
    /**
     * the topology name you want to submit to the cluster.
     */
    private String topologyName;

    /**
     * the timeout of topology you want to submit to the cluster.
     */
    private int timeoutMs = Testing.TEST_TIMEOUT_MS;

    public MockedSources getMockedSources() {
        return mockedSources;
    }

    public void setMockedSources(MockedSources mockedSources) {
        if (mockedSources == null) {
            mockedSources = new MockedSources();
        }

        this.mockedSources = mockedSources;
    }

    public Map<String, Object> getStormConf() {
        return topoConf;
    }

    public void setStormConf(Map<String, Object> topoConf) {
        if (topoConf == null) {
            topoConf = new Config();
        }
        this.topoConf = topoConf;
    }

    public boolean getCleanupState() {
        return cleanupState;
    }

    public void setCleanupState(Boolean cleanupState) {
        if (cleanupState == null) {
            cleanupState = true;
        }
        this.cleanupState = cleanupState;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public Integer getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(Integer timeoutMs) {
        if (timeoutMs == null) {
            timeoutMs = Testing.TEST_TIMEOUT_MS;
        }
        this.timeoutMs = timeoutMs;
    }
}
