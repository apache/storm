package org.apache.storm.callback;

import org.apache.zookeeper.Watcher;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public interface WatcherCallBack {
    public void execute(Watcher.Event.KeeperState state, Watcher.Event.EventType type, String path);
}