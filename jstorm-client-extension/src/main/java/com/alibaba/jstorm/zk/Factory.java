package com.alibaba.jstorm.zk;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class Factory extends NIOServerCnxnFactory {

	public Factory(InetSocketAddress addr, int maxcc) throws IOException {
		super();
		this.configure(addr, maxcc);
	}

	public ZooKeeperServer getZooKeeperServer() {
		return zkServer;
	}

}
