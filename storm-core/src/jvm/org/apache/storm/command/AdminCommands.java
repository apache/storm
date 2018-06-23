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

package org.apache.storm.command;

import com.google.common.collect.Sets;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.shade.org.apache.zookeeper.ZkCli;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminCommands {
    private static final Logger LOG = LoggerFactory.getLogger(AdminCommands.class);

    public interface AdminCommand {
        
        /**
         * Run the command, this will be called at most once.
         */
        void run(String [] args, Map<String, Object> conf, String command) throws Exception;

        /**
         * Print a help message to out.  typically this should be in the form of.
         * command arguments:
         *     description of command
         *     argument - description
         */
        void printCliHelp(String command, PrintStream out);
    }

    private static class RemoveCorruptTopologies implements AdminCommand {
        @Override
        public void run(String[] args, Map<String, Object> conf, String command) throws Exception {
            BlobStore nimbusBlobStore = ServerUtils.getNimbusBlobStore(conf, NimbusInfo.fromConf(conf), null);
            IStormClusterState stormClusterState = ClusterUtils.mkStormClusterState(conf, new ClusterStateContext(DaemonType.NIMBUS, conf));

            Set<String> blobStoreTopologyIds = nimbusBlobStore.filterAndListKeys(key -> ConfigUtils.getIdFromBlobKey(key));
            Set<String> activeTopologyIds = new HashSet<>(stormClusterState.activeStorms());
            Sets.SetView<String> diffTopology = Sets.difference(activeTopologyIds, blobStoreTopologyIds);
            LOG.info("active-topology-ids [{}] blob-topology-ids [{}] diff-topology [{}]",
                activeTopologyIds, blobStoreTopologyIds, diffTopology);
            for (String corruptId : diffTopology) {
                stormClusterState.removeStorm(corruptId);
            }
        }

        @Override
        public void printCliHelp(String command, PrintStream out) {
            out.println(command + ":");
            out.println("\tKill topologies that appear to be corrupted (missing blobs).");
        }
    }

    private static class Help implements AdminCommand {

        @Override
        public void run(String[] args, Map<String, Object> conf, String command) {
            if (args.length <= 0) {
                help(null, System.out);
            } else {
                for (String cn : args) {
                    AdminCommand c = COMMANDS.get(cn);
                    if (c == null) {
                        throw new IllegalArgumentException(cn + " is not a supported command");
                    }
                    c.printCliHelp(cn, System.out);
                }
            }
        }

        @Override
        public void printCliHelp(String command, PrintStream out) {
            out.println(command + " [<command>...]:");
            out.println("\tPrint a help message about one or more commands.  If not commands are given, print all");
        }
    }

    public static final Map<String, AdminCommand> COMMANDS = new HashMap<>();

    static {
        COMMANDS.put("remove_corrupt_topologies", new RemoveCorruptTopologies());
        COMMANDS.put("zk_cli", new ZkCli());
        COMMANDS.put("help", new Help());
    }

    static void help(String message, PrintStream out) {
        if (message != null) {
            out.println(message);
        }
        out.println("storm admin <command> [args]");
        out.println();
        for (Map.Entry<String, AdminCommand> entry: COMMANDS.entrySet()) {
            entry.getValue().printCliHelp(entry.getKey(), out);
            out.println();
        }
    }

    /**
     * Main entry point to the admin command.
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            help("Missing command.", System.err);
            System.exit(1);
        }
        String commandName = args[0].toLowerCase();
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        AdminCommand command = COMMANDS.get(commandName);
        if (command == null) {
            help(commandName + " is not a supported command", System.err);
            System.exit(2);
        }

        try {
            command.run(newArgs, Utils.readStormConfig(), commandName);
        } catch (Exception e) {
            LOG.error("Error while running command {}", commandName, e);
            help("Error while running " + commandName + " " + e.getMessage(), System.err);
            System.exit(3);
        }
    }
}
