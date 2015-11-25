/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.zk;

import java.util.List;
import java.util.Map;

import com.alibaba.jstorm.cluster.Cluster;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.cluster.DistributedClusterState;
import com.google.common.collect.Maps;

public class ZkTool {
    private static Logger LOG = Logger.getLogger(ZkTool.class);

    public static final String READ_CMD = "read";

    public static final String RM_CMD = "rm";

    public static final String LIST_CMD = "list";

    public static final String CLEAN_CMD = "clean";

    public static void usage() {
        LOG.info("Read ZK node's data, please do as following:");
        LOG.info(ZkTool.class.getName() + " read zkpath");

        LOG.info("\nDelete topology backup assignment, please do as following:");
        LOG.info(ZkTool.class.getName() + " rm topologyname");

        LOG.info("\nlist subdirectory of zkPath , please do as following:");
        LOG.info(ZkTool.class.getName() + " list zkpath");

        LOG.info("\nDelete all nodes about a topologyId of zk , please do as following:");
        LOG.info(ZkTool.class.getName() + " clean topologyId");

    }

    public static String getData(DistributedClusterState zkClusterState,
                                 String path) throws Exception {
        byte[] data = zkClusterState.get_data(path, false);
        if (data == null || data.length == 0) {
            return null;
        }

        Object obj = Utils.deserialize(data, null);

        return obj.toString();
    }


    public static void list(String path) {
        DistributedClusterState zkClusterState = null;

        try {
            conf.put(Config.STORM_ZOOKEEPER_ROOT, "/");

            zkClusterState = new DistributedClusterState(conf);

            List<String>  children = zkClusterState.get_children(path, false);
            if (children == null || children.isEmpty() ) {
                LOG.info("No children of " + path);
            }
            else
            {
                StringBuilder sb = new StringBuilder();
                sb.append("Zk node children of " + path + "\n");
                for (String str : children){
                    sb.append(" " + str + ",");
                }
                sb.append("\n");
                LOG.info(sb.toString());
            }
        } catch (Exception e) {
            if (zkClusterState == null) {
                LOG.error("Failed to connect ZK ", e);
            } else {
                LOG.error("Failed to list children of  " + path + "\n", e);
            }
        } finally {
            if (zkClusterState != null) {
                zkClusterState.close();
            }
        }
    }
    /**
     * warnning! use this method cann't delete zkCache right now because of
     *  new DistributedClusterState(conf)
     */
    public static void cleanTopology( String topologyId){
        DistributedClusterState zkClusterState = null;
        try {
            zkClusterState = new DistributedClusterState(conf);
            String rootDir = String.valueOf(conf.get(Config.STORM_ZOOKEEPER_ROOT));
            String assignmentPath = "/assignments/"+ topologyId;
            String stormBase = "/topology/"+ topologyId;
            String taskbeats = "/taskbeats/"+ topologyId;
            String tasks = "/tasks/"+ topologyId;
            String taskerrors = "/taskerrors/"+ topologyId;
            String monitor = "/monitor/"+ topologyId;
            if (zkClusterState.node_existed(assignmentPath, false)){
                try {
                    zkClusterState.delete_node(assignmentPath);
                } catch (Exception e) {
                    LOG.error("Could not remove assignments for " + topologyId, e);
                }
            }else {
                LOG.info(" node of " + rootDir + assignmentPath + " isn't existed ");

            }

            if (zkClusterState.node_existed(stormBase, false)){
                try {
                    zkClusterState.delete_node(stormBase);
                } catch (Exception e) {
                    LOG.error("Failed to remove storm base for " + topologyId, e);
                }
            }else {
                LOG.info(" node of " + rootDir + stormBase + " isn't existed ");

            }

            if (zkClusterState.node_existed(taskbeats, false)){
                try {
                    zkClusterState.delete_node(taskbeats);
                } catch (Exception e) {
                    LOG.error("Failed to remove taskbeats for " + topologyId, e);
                }
            }else {
                LOG.info(" node of " + rootDir + taskbeats + " isn't existed ");

            }

            if (zkClusterState.node_existed(tasks, false)){
                try {
                    zkClusterState.delete_node(tasks);
                } catch (Exception e) {
                    LOG.error("Failed to remove tasks for " + topologyId, e);
                }
            }else {
                LOG.info(" node of " + rootDir + tasks + " isn't existed ");

            }

            if (zkClusterState.node_existed(taskerrors, false)){
                try {
                    zkClusterState.delete_node(taskerrors);
                } catch (Exception e) {
                    LOG.error("Failed to remove taskerrors for " + topologyId, e);
                }
            }else {
                LOG.info(" node of " + rootDir + taskerrors + " isn't existed ");

            }

            if (zkClusterState.node_existed(monitor, false)){
                try {
                    zkClusterState.delete_node(monitor);
                } catch (Exception e) {
                    LOG.error("Failed to remove monitor for " + topologyId, e);
                }
            }else {
                LOG.info(" node of " + rootDir + monitor + " isn't existed ");

            }
        } catch (Exception e) {
            if (zkClusterState == null) {
                LOG.error("Failed to connect ZK ", e);
            } else {
                LOG.error("Failed to clean  topolodyId: " + topologyId + "\n", e);
            }
        } finally {
            if (zkClusterState != null) {
                zkClusterState.close();
            }
        }

    }

    public static void readData(String path) {

        DistributedClusterState zkClusterState = null;

        try {
            conf.put(Config.STORM_ZOOKEEPER_ROOT, "/");

            zkClusterState = new DistributedClusterState(conf);

            String data = getData(zkClusterState, path);
            if (data == null) {
                LOG.info("No data of " + path);
            }

            StringBuilder sb = new StringBuilder();

            sb.append("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
            sb.append("Zk node " + path + "\n");
            sb.append("Readable data:" + data + "\n");
            sb.append("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");

            LOG.info(sb.toString());

        } catch (Exception e) {
            if (zkClusterState == null) {
                LOG.error("Failed to connect ZK ", e);
            } else {
                LOG.error("Failed to read data " + path + "\n", e);
            }
        } finally {
            if (zkClusterState != null) {
                zkClusterState.close();
            }
        }
    }

    public static void rmBakTopology(String topologyName) {

        DistributedClusterState zkClusterState = null;

        try {

            zkClusterState = new DistributedClusterState(conf);

            String path = Cluster.ASSIGNMENTS_BAK_SUBTREE;
            List<String> bakTopologys =
                    zkClusterState.get_children(path, false);

            for (String tid : bakTopologys) {
                if (tid.equals(topologyName)) {
                    LOG.info("Find backup " + topologyName);

                    String topologyPath = assignment_bak_path(topologyName);
                    zkClusterState.delete_node(topologyPath);

                    LOG.info("Successfully delete topology " + topologyName
                            + " backup Assignment");

                    return;
                }
            }

            LOG.info("No backup topology " + topologyName + " Assignment");

        } catch (Exception e) {
            if (zkClusterState == null) {
                LOG.error("Failed to connect ZK ", e);
            } else {
                LOG.error("Failed to delete old topology " + topologyName
                        + "\n", e);
            }
        } finally {
            if (zkClusterState != null) {
                zkClusterState.close();
            }
        }

    }

    private static Map conf;

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        if (args.length < 2) {
            LOG.info("Invalid parameter");
            usage();
            return;
        }

        conf = Utils.readStormConfig();

        if (args[0].equalsIgnoreCase(READ_CMD)) {

            readData(args[1]);

        } else if (args[0].equalsIgnoreCase(RM_CMD)) {
            rmBakTopology(args[1]);
        } else if (args[0].equalsIgnoreCase(LIST_CMD)) {
            list(args[1]);
        } else if (args[0].equalsIgnoreCase(CLEAN_CMD)) {
            cleanTopology(args[1]);
        }

    }

    /*******************************************************************/

    public static String assignment_bak_path(String id) {
        return Cluster.ASSIGNMENTS_BAK_SUBTREE + Cluster.ZK_SEPERATOR
                + id;
    }

    @SuppressWarnings("rawtypes")
    public static ClusterState mk_distributed_cluster_state(Map _conf)
            throws Exception {
        return new DistributedClusterState(_conf);
    }

    public static Map<String, String> get_followers(ClusterState cluster_state)
            throws Exception {
        Map<String, String> ret = Maps.newHashMap();
        List<String> followers =
                cluster_state.get_children(Cluster.NIMBUS_SLAVE_SUBTREE,
                        false);
        if (followers == null || followers.size() == 0) {
            return ret;
        }
        for (String follower : followers) {
            if (follower != null) {
                String uptime =
                        new String(cluster_state.get_data(
                                Cluster.NIMBUS_SLAVE_SUBTREE
                                        + Cluster.ZK_SEPERATOR + follower,
                                false));
                ret.put(follower, uptime);
            }
        }
        return ret;
    }

    // public static List<String> get_follower_hosts(ClusterState cluster_state)
    // throws Exception {
    // List<String> followers = cluster_state.get_children(
    // ZkConstant.NIMBUS_SLAVE_SUBTREE, false);
    // if (followers == null || followers.size() == 0) {
    // return Lists.newArrayList();
    // }
    // return followers;
    // }
    //
    // public static List<String> get_follower_hbs(ClusterState cluster_state)
    // throws Exception {
    // List<String> ret = Lists.newArrayList();
    // List<String> followers = get_follower_hosts(cluster_state);
    // for (String follower : followers) {
    // ret.add(new String(cluster_state.get_data(ZkConstant.NIMBUS_SLAVE_SUBTREE
    // + ZkConstant.ZK_SEPERATOR + follower, false)));
    // }
    // return ret;
    // }

}
