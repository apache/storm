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

package org.apache.storm.zookeeper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.StormTimer;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.storm.shade.org.apache.curator.framework.recipes.leader.Participant;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElectorImp implements ILeaderElector {
    private static final Logger LOG = LoggerFactory.getLogger(LeaderElectorImp.class);
    private final CuratorFramework zk;
    private final String leaderLockPath = "/leader-lock";
    private final String id;
    private final AtomicReference<LeaderLatch> leaderLatch;
    private final LeaderListenerCallbackFactory leaderListenerCallbackFactory;
    private final StormTimer timer;

    public LeaderElectorImp(CuratorFramework zk, String id, LeaderListenerCallbackFactory leaderListenerCallbackFactory) {
        this.zk = zk;
        this.id = id;
        this.leaderLatch = new AtomicReference<>(new LeaderLatch(zk, leaderLockPath, id));
        this.leaderListenerCallbackFactory = leaderListenerCallbackFactory;
        this.timer = new StormTimer("leader-elector-timer", Utils.createDefaultUncaughtExceptionHandler());
    }

    @Override
    public void prepare(Map<String, Object> conf) {
        // no-op for zookeeper implementation
    }

    @Override
    public void addToLeaderLockQueue() throws Exception {
        // if this latch is closed, we need to create new instance.
        if (LeaderLatch.State.CLOSED.equals(leaderLatch.get().getState())) {
            LeaderLatch latch = new LeaderLatch(zk, leaderLockPath, id);
            latch.addListener(leaderListenerCallbackFactory.create(this));
            latch.start();
            leaderLatch.set(latch);
            LOG.info("LeaderLatch was in closed state. Reset the leaderLatch, and queued for leader lock.");
        }
        // If the latch is not started yet, start it
        if (LeaderLatch.State.LATENT.equals(leaderLatch.get().getState())) {
            leaderLatch.get().addListener(leaderListenerCallbackFactory.create(this));
            leaderLatch.get().start();
            LOG.info("Queued up for leader lock.");
        } else {
            LOG.info("Node already in queue for leader lock.");
        }
    }

    @Override
    public void quitElectionFor(int delayMs) throws Exception {
        removeFromLeaderLockQueue();
        timer.schedule(delayMs, () -> {
            try {
                addToLeaderLockQueue();
            } catch (Exception e) {
                throw Utils.wrapInRuntime(e);
            }
        }, false, 0); //Don't error if timer is shut down, happens when the elector is closed.
    }
    
    private void removeFromLeaderLockQueue() throws Exception {
        if (LeaderLatch.State.STARTED.equals(leaderLatch.get().getState())) {
            leaderLatch.get().close();
            LOG.info("Removed from leader lock queue.");
        } else {
            LOG.info("Leader latch is not started so no removeFromLeaderLockQueue needed.");
        }
    }

    @Override
    public boolean isLeader() throws Exception {
        return leaderLatch.get().hasLeadership();
    }

    @Override
    @VisibleForTesting
    public boolean awaitLeadership(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return leaderLatch.get().await(timeout, timeUnit);
    }

    @Override
    public NimbusInfo getLeader() {
        try {
            return Zookeeper.toNimbusInfo(leaderLatch.get().getLeader());
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    @Override
    public List<NimbusInfo> getAllNimbuses() throws Exception {
        List<NimbusInfo> nimbusInfos = new ArrayList<>();
        Collection<Participant> participants = leaderLatch.get().getParticipants();
        for (Participant participant : participants) {
            nimbusInfos.add(Zookeeper.toNimbusInfo(participant));
        }
        return nimbusInfos;
    }

    @Override
    public void close() throws Exception {
        timer.close();
        removeFromLeaderLockQueue();
    }
}
