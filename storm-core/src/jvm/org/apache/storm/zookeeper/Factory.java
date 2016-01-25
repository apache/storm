package org.apache.storm.zookeeper;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class Factory extends NIOServerCnxnFactory {

    public Factory(InetSocketAddress addr, int maxcc) throws IOException {
        super();
        this.configure(addr, maxcc);
    }

    public ZooKeeperServer getZooKeeperServer() {
        return zkServer;
    }

}
