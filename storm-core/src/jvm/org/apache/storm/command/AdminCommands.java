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
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.nimbus.Nimbus;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.shade.org.apache.zookeeper.ZkCli;
import org.apache.storm.thrift.TBase;
import org.apache.storm.thrift.TFieldIdEnum;
import org.apache.storm.thrift.meta_data.FieldMetaData;
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
            try (BlobStore nimbusBlobStore = ServerUtils.getNimbusBlobStore(conf, NimbusInfo.fromConf(conf), null)) {
                IStormClusterState stormClusterState = ClusterUtils.mkStormClusterState(conf,
                        new ClusterStateContext(DaemonType.NIMBUS, conf));

                Set<String> blobStoreTopologyIds = nimbusBlobStore.filterAndListKeys(key -> ConfigUtils.getIdFromBlobKey(key));
                Set<String> activeTopologyIds = new HashSet<>(stormClusterState.activeStorms());
                Sets.SetView<String> diffTopology = Sets.difference(activeTopologyIds, blobStoreTopologyIds);
                LOG.info("active-topology-ids [{}] blob-topology-ids [{}] diff-topology [{}]",
                    activeTopologyIds, blobStoreTopologyIds, diffTopology);
                for (String corruptId : diffTopology) {
                    stormClusterState.removeStorm(corruptId);
                }
            }
        }

        @Override
        public void printCliHelp(String command, PrintStream out) {
            out.println(command + ":");
            out.println("\tKill topologies that appear to be corrupted (missing blobs).");
        }
    }

    private static class CredentialsDebug implements AdminCommand {
        @Override
        public void run(String[] args, Map<String, Object> conf, String command) throws Exception {
            // We are pretending to be nimbus here.
            IStormClusterState state = ClusterUtils.mkStormClusterState(conf, new ClusterStateContext(DaemonType.NIMBUS, conf));
            for (String topologyId: args) {
                System.out.println(topologyId + ":");
                Credentials creds = state.credentials(topologyId, null);
                if (creds != null) {
                    for (String key : creds.get_creds().keySet()) {
                        System.out.println("\t" + key);
                    }
                }
            }
        }

        @Override
        public void printCliHelp(String command, PrintStream out) {
            out.println(command + " topology_id:");
            out.println("\tPrint the credential keys for a topology.");
        }
    }

    /**
     * Print value in a human readable format.
     * @param value what to print.
     * @return a human readable string
     */
    public static String prettyPrint(TBase value) {
        StringBuilder builder = new StringBuilder();
        prettyPrint(value, 0, builder);
        return builder.toString();
    }

    private static void prettyPrint(TBase value, int depth, StringBuilder out) {
        if (value == null) {
            println(out, depth, "null");
            return;
        }
        println(out, depth, "{");
        prettyPrintFields(value, depth + 1, out);
        println(out, depth, "}");
    }

    private static void println(StringBuilder out, int depth, Object value) {
        for (int i = 0; i < depth; i++) {
            out.append("\t");
        }
        out.append(value);
        out.append("\n");
    }

    private static void prettyPrintFields(TBase value, int depth, StringBuilder out) {
        for (Map.Entry<? extends TFieldIdEnum, FieldMetaData> entry : FieldMetaData.getStructMetaDataMap(value.getClass()).entrySet()) {
            TFieldIdEnum key = entry.getKey();
            if (!value.isSet(key)) {
                println(out, depth, key.getFieldName() + ": not set");
            } else {
                Object o = value.getFieldValue(key);
                prettyPrintKeyValue(key.getFieldName(), o, depth, out);
            }
        }
    }

    private static String keyStr(String key) {
        return key == null ? "" : (key + ": ");
    }

    private static void prettyPrintKeyValue(String key, Object o, int depth, StringBuilder out) {
        //Special cases for storm...
        if ("json_conf".equals(key) && o instanceof String) {
            try {
                o = Utils.parseJson((String) o);
            } catch (Exception e) {
                LOG.error("Could not parse json_conf as JSON", e);
            }
        }
        if (o instanceof TBase) {
            println(out, depth, keyStr(key) + "{");
            prettyPrintFields((TBase) o, depth + 1, out);
            println(out, depth, "}");
        } else if (o instanceof Map) {
            println(out, depth, keyStr(key) + "{");
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) o).entrySet()) {
                prettyPrintKeyValue(entry.getKey().toString(), entry.getValue(), depth + 1, out);
            }
            println(out, depth, "}");
        } else if (o instanceof Collection) {
            println(out, depth, keyStr(key) + "[");
            for (Object sub: (Collection) o) {
                prettyPrintKeyValue(null, sub, depth + 1, out);
            }
            println(out, depth, "]");
        } else if (o instanceof String) {
            println(out, depth, keyStr(key) + "\"" + o + "\"");
        } else {
            println(out, depth, keyStr(key) + o);
        }
    }

    private static class PrintTopo implements AdminCommand {

        @Override
        public void run(String[] args, Map<String, Object> conf, String command) throws Exception {
            for (String arg: args) {
                System.out.println(arg + ":");
                StormTopology topo;
                File f = new File(arg);
                if (f.exists()) {
                    topo = Utils.deserialize(FileUtils.readFileToByteArray(f), StormTopology.class);
                } else { //assume it is a topology id
                    final String key = ConfigUtils.masterStormCodeKey(arg);
                    try (BlobStore store = ServerUtils.getNimbusBlobStore(conf, NimbusInfo.fromConf(conf), null)) {
                        topo = Utils.deserialize(store.readBlob(key, Nimbus.NIMBUS_SUBJECT), StormTopology.class);
                    }
                }

                System.out.println(prettyPrint(topo));
            }
        }

        @Override
        public void printCliHelp(String command, PrintStream out) {
            out.println(command + " [topology_id|file]*:");
            out.println("\tPrint a human readable version of the topology");
        }
    }

    private static class PrintSupervisors implements AdminCommand {
        @Override
        public void run(String[] args, Map<String, Object> conf, String command) throws Exception {
            IStormClusterState stormClusterState = ClusterUtils.mkStormClusterState(conf, new ClusterStateContext(DaemonType.NIMBUS, conf));
            Map<String, SupervisorInfo> infos = stormClusterState.allSupervisorInfo();
            if (args.length <= 0) {
                for (Map.Entry<String, SupervisorInfo> entry: infos.entrySet()) {
                    System.out.println(entry.getKey() + ":");
                    System.out.println(prettyPrint(entry.getValue()));
                }
            } else {
                for (String arg : args) {
                    System.out.println(arg + ":");
                    System.out.println(prettyPrint(infos.get(arg)));
                }
            }
        }

        @Override
        public void printCliHelp(String command, PrintStream out) {
            out.println(command + " [supervisor_id]*:");
            out.println("\tPrint a human readable version of the supervisor info(s). Print all if no args");
        }
    }

    private static class PrintAssignments implements AdminCommand {
        @Override
        public void run(String[] args, Map<String, Object> conf, String command) throws Exception {
            IStormClusterState stormClusterState = ClusterUtils.mkStormClusterState(conf, new ClusterStateContext(DaemonType.NIMBUS, conf));
            stormClusterState.syncRemoteAssignments(null);
            stormClusterState.syncRemoteIds(null);
            stormClusterState.setAssignmentsBackendSynchronized();
            Map<String, Assignment> infos = stormClusterState.assignmentsInfo();
            if (args.length <= 0) {
                for (Map.Entry<String, Assignment> entry: infos.entrySet()) {
                    System.out.println(entry.getKey() + ":");
                    System.out.println(prettyPrint(entry.getValue()));
                }
            } else {
                for (String arg : args) {
                    System.out.println(arg + ":");
                    System.out.println(prettyPrint(infos.get(arg)));
                }
            }
        }

        @Override
        public void printCliHelp(String command, PrintStream out) {
            out.println(command + " [topology_id]*:");
            out.println("\tPrint a human readable version of the topologies assignment info(s). Print all if no args");
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
        COMMANDS.put("creds", new CredentialsDebug());
        COMMANDS.put("help", new Help());
        COMMANDS.put("print_topo", new PrintTopo());
        COMMANDS.put("print_super", new PrintSupervisors());
        COMMANDS.put("print_assignment", new PrintAssignments());
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
