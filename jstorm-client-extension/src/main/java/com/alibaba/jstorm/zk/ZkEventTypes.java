package com.alibaba.jstorm.zk;

import java.util.HashMap;

import org.apache.zookeeper.Watcher;

public class ZkEventTypes {

	private static HashMap<Watcher.Event.EventType, String> map;

	static {
		map = new HashMap<Watcher.Event.EventType, String>();

		map.put(Watcher.Event.EventType.None, ":none");
		map.put(Watcher.Event.EventType.NodeCreated, ":node-created");
		map.put(Watcher.Event.EventType.NodeDeleted, ":node-deleted");
		map.put(Watcher.Event.EventType.NodeDataChanged, ":node-data-changed");
		map.put(Watcher.Event.EventType.NodeChildrenChanged,
				":node-children-changed");

	}

	public static String getStateName(Watcher.Event.EventType type) {
		return map.get(type);
	}

}
