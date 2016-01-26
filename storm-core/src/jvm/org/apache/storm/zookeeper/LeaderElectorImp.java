package org.apache.storm.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class LeaderElectorImp implements ILeaderElector {
    private static Logger LOG = LoggerFactory.getLogger(LeaderElectorImp.class);
    private Map conf;
    private List<String> servers;
    private CuratorFramework zk;
    private String leaderlockPath;
    private String id;
    private AtomicReference<LeaderLatch> leaderLatch;
    private AtomicReference<LeaderLatchListener> leaderLatchListener;

    public LeaderElectorImp(Map conf, List<String> servers, CuratorFramework zk, String leaderlockPath, String id, AtomicReference<LeaderLatch> leaderLatch,
            AtomicReference<LeaderLatchListener> leaderLatchListener) {
        this.conf = conf;
        this.servers = servers;
        this.zk = zk;
        this.leaderLatch = leaderLatch;
        this.id = id;
        this.leaderLatch = leaderLatch;
        this.leaderLatchListener = leaderLatchListener;
    }

    @Override
    public void prepare(Map conf) {
        LOG.info("no-op for zookeeper implementation");
    }

    @Override
    public void addToLeaderLockQueue() {
        // if this latch is already closed, we need to create new instance.
        try {
            if (LeaderLatch.State.CLOSED.equals(leaderLatch.get().getState())) {
                leaderLatch.set(new LeaderLatch(zk, leaderlockPath));
                leaderLatchListener.set(Zookeeper.leaderLatchListenerImpl(conf, zk, leaderLatch.get()));
            }
            // Only if the latch is not already started we invoke start
            if (LeaderLatch.State.LATENT.equals(leaderLatch.get().getState())) {
                leaderLatch.get().addListener(leaderLatchListener.get());
                leaderLatch.get().start();
                LOG.info("Queued up for leader lock.");
            } else {
                LOG.info("Node already in queue for leader lock.");
            }
        } catch (Exception e) {
            LOG.warn("failed to add LeaderLatchListener", e);
        }

    }

    @Override
    // Only started latches can be closed.
    public void removeFromLeaderLockQueue() {
        if (LeaderLatch.State.STARTED.equals(leaderLatch.get().getState())) {
            try {
                leaderLatch.get().close();
                LOG.info("Removed from leader lock queue.");
            } catch (IOException e) {
                LOG.warn("failed to  close leaderLatch!!");
            }

        } else {
            LOG.info("leader latch is not started so no removeFromLeaderLockQueue needed.");
        }
    }

    @Override
    public boolean isLeader() throws Exception {
        return leaderLatch.get().hasLeadership();
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
    public List<NimbusInfo> getAllNimbuses() {
        List<NimbusInfo> nimbusInfos = new ArrayList<>();
        try {
            Collection<Participant> participants = leaderLatch.get().getParticipants();
            for (Participant participant : participants) {
                nimbusInfos.add(Zookeeper.toNimbusInfo(participant));
            }
        } catch (Exception e) {
            LOG.warn("failed to get nimbuses", e);
        }
        return nimbusInfos;
    }

    @Override
    public void close() {
        LOG.info("closing zookeeper connection of leader elector.");
        zk.close();
    }
}
