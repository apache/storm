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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.cluster.DistributedClusterState;
import com.google.common.collect.Maps;

public class ZkTool {
    private static Logger LOG = LoggerFactory.getLogger(ZkTool.class);

    public static final String READ_CMD = "read";

    public static final String RM_CMD = "rm";

    public static void usage() {
        LOG.info("Read ZK node's data, please do as following:");
        LOG.info(ZkTool.class.getName() + " read zkpath");

        LOG.info("\nDelete topology backup assignment, please do as following:");
        LOG.info(ZkTool.class.getName() + " rm topologyname");
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

                    String topologyPath =
                            Cluster.assignment_bak_path(topologyName);
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
        }

    }

    /*******************************************************************/

    @SuppressWarnings("rawtypes")
    public static ClusterState mk_distributed_cluster_state(Map _conf)
            throws Exception {
        return new DistributedClusterState(_conf);
    }

    public static Map<String, String> get_followers(ClusterState cluster_state)
            throws Exception {
        Map<String, String> ret = Maps.newHashMap();
        List<String> followers =
                cluster_state.get_children(Cluster.NIMBUS_SLAVE_SUBTREE, false);
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
