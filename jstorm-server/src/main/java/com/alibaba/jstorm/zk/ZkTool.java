package com.alibaba.jstorm.zk;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.DistributedClusterState;

public class ZkTool {
	private static Logger LOG = Logger.getLogger(ZkTool.class);

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

		Object obj = Utils.deserialize(data);

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
			List<String> bakTopologys = zkClusterState
					.get_children(path, false);

			for (String tid : bakTopologys) {
				if (tid.equals(topologyName)) {
					LOG.info("Find backup " + topologyName);

					String topologyPath = Cluster
							.assignment_bak_path(topologyName);
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

}
