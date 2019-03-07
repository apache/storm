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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;

public class MockLeaderElector implements ILeaderElector {
    private final boolean isLeader;
    private final NimbusInfo leaderAddress;

    public MockLeaderElector() {
        this(true, "test-host", 9999);
    }

    public MockLeaderElector(boolean isLeader) {
        this(isLeader, "test-host", 9999);
    }

    public MockLeaderElector(boolean isLeader, String host, int port) {
        this.isLeader = isLeader;
        this.leaderAddress = new NimbusInfo(host, port, true);
    }

    @Override
    public void prepare(Map<String, Object> conf) {
        //NOOP
    }

    @Override
    public void addToLeaderLockQueue() throws Exception {
        //NOOP
    }

    @Override
    public void quitElectionFor(int delayMs) throws Exception {
        //NOOP
    }

    @Override
    public boolean isLeader() throws Exception {
        return isLeader;
    }

    @Override
    public boolean awaitLeadership(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return isLeader;
    }

    @Override
    public NimbusInfo getLeader() {
        return leaderAddress;
    }

    @Override
    public List<NimbusInfo> getAllNimbuses() throws Exception {
        return Arrays.asList(leaderAddress);
    }

    @Override
    public void close() {
        //NOOP
    }
}
