package org.apache.storm.callback;

import org.apache.storm.zookeeper.ZkEventTypes;
import org.apache.storm.zookeeper.ZkKeeperStates;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class DefaultWatcherCallBack implements WatcherCallBack {

    private static Logger LOG = LoggerFactory.getLogger(DefaultWatcherCallBack.class);

    @Override
    public void execute(Watcher.Event.KeeperState state, Watcher.Event.EventType type, String path) {
        LOG.info("Zookeeper state update:" + ZkKeeperStates.getStateName(state) + "," + ZkEventTypes.getStateName(type) + "," + path);
    }

}

