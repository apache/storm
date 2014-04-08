package com.alibaba.jstorm.zk;

import java.util.HashMap;

import org.apache.zookeeper.Watcher;

public class ZkKeeperStates {

	private static HashMap<Watcher.Event.KeeperState, String> map;

	static {
		map = new HashMap<Watcher.Event.KeeperState, String>();

		map.put(Watcher.Event.KeeperState.AuthFailed, ":auth-failed");
		map.put(Watcher.Event.KeeperState.SyncConnected, ":connected");
		map.put(Watcher.Event.KeeperState.Disconnected, ":disconnected");
		map.put(Watcher.Event.KeeperState.Expired, ":expired");
	}

	public static String getStateName(Watcher.Event.KeeperState state) {
		return map.get(state);
	}

}
