/**
 * Licensed to the Apache Software Foundation (ASF)
 * under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

//This is a hack to allow ZooKeeperMain to be called by this command.

package org.apache.storm.shade.org.apache.zookeeper;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.command.AdminCommands;
import org.apache.storm.command.CLI;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Stat;
import org.apache.storm.utils.ObjectReader;

public class ZkCli implements AdminCommands.AdminCommand {
    @Override
    public void run(String[] args, Map<String, Object> conf, String command) throws Exception {
        List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        int port = ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_PORT));
        String root = (String) conf.get(Config.STORM_ZOOKEEPER_ROOT);
        Map<String, Object> cl = CLI.opt("s", "server", null, CLI.AS_STRING, CLI.LAST_WINS)
            .opt("t", "time-out", ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                CLI.AS_INT, CLI.LAST_WINS)
            .boolOpt("w", "write")
            .boolOpt("n", "no-root")
            .opt("j", "jaas", conf.get("java.security.auth.login.config"), CLI.AS_STRING, CLI.LAST_WINS)
            .boolOpt("h", "help")
            .parse(args);

        if ((Boolean) cl.get("h")) {
            printCliHelp(command, System.out);
            return;
        }

        String jaas = (String) cl.get("j");
        if (jaas != null && !jaas.isEmpty()) {
            System.setProperty("java.security.auth.login.config", jaas);
        }

        String connectionString = (String) cl.get("s");
        if (connectionString == null) {
            StringBuilder sb = new StringBuilder();
            boolean isFirst = true;
            for (String zkServer : servers) {
                if (!isFirst) {
                    sb.append(',');
                }
                isFirst = false;
                sb.append(zkServer).append(':').append(port);
            }
            if (!(Boolean) cl.get("n")) {
                sb.append(root);
            }
            connectionString = sb.toString();
        }

        boolean readOnly = !(Boolean) cl.get("w");
        int timeout = (Integer) cl.get("t");
        ZooKeeper zk;
        if (readOnly) {
            zk = new ReadOnlyZookeeper(connectionString, timeout, watchedEvent -> { });
        } else {
            zk = new ZooKeeper(connectionString, timeout, watchedEvent -> { });
        }
        ZooKeeperMain main = new ZooKeeperMain(zk);
        main.run();
    }

    @Override
    public void printCliHelp(String command, PrintStream out) {
        out.println(command + " [<opts>]:");
        out.println("\tStart a zookeeper shell");
        out.println();
        out.println("\t-s --server <connection string>: Set the connection string to use, defaults to storm connection string.");
        out.println("\t-t --time-out <timeout>:         Set the timeout to use, defaults to storm zookeeper timeout.");
        out.println("\t-w --write:                      Allow for writes, defaults to read only, we don't want to cause problems.");
        out.println("\t-n --no-root:                    Don't include the storm root on the default connection string.");
        out.println("\t-j --jaas <jaas_file>:           Include a jaas file that should be used when authenticating with\n\t\t"
            + "ZK defaults to the java.security.auth.login.config conf.");
    }

    private static class ReadOnlyZookeeper extends ZooKeeper {
        ReadOnlyZookeeper(String connectionString, int timeout, Watcher watcher) throws IOException {
            super(connectionString, timeout, watcher);
        }

        @Override
        public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException {
            throw KeeperException.create(KeeperException.Code.NOTREADONLY, path);
        }

        @Override
        public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
            throw new IllegalArgumentException("In Read Only Mode");
        }

        @Override
        public void delete(String path, int version) throws KeeperException {
            throw KeeperException.create(KeeperException.Code.NOTREADONLY, path);
        }

        @Override
        public void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx) {
            throw new IllegalArgumentException("In Read Only Mode");
        }

        @Override
        public List<OpResult> multi(Iterable<Op> ops) throws KeeperException {
            throw KeeperException.create(KeeperException.Code.NOTREADONLY, "multi opt");
        }

        @Override
        public Transaction transaction() {
            throw new IllegalArgumentException("In Read Only Mode");
        }

        @Override
        public Stat setData(String path, byte[] data, int version) throws KeeperException {
            throw KeeperException.create(KeeperException.Code.NOTREADONLY, path);
        }

        @Override
        public void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx) {
            throw new IllegalArgumentException("In Read Only Mode");
        }

        @Override
        public Stat setACL(String path, List<ACL> acl, int version) throws KeeperException {
            throw KeeperException.create(KeeperException.Code.NOTREADONLY, path);
        }

        @Override
        public void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx) {
            throw new IllegalArgumentException("In Read Only Mode");
        }
    }
}
