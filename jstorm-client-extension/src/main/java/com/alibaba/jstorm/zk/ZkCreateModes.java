package com.alibaba.jstorm.zk;

import java.util.HashMap;

import org.apache.zookeeper.CreateMode;

public class ZkCreateModes {

	private static HashMap<CreateMode, String> map;

	static {
		map = new HashMap<CreateMode, String>();
		map.put(CreateMode.EPHEMERAL, ":ephemeral");
		map.put(CreateMode.PERSISTENT, ":persistent");
	}

	public static String getStateName(CreateMode mode) {
		return map.get(mode);
	}

}
