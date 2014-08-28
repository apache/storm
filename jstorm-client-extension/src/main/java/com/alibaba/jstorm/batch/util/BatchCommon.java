package com.alibaba.jstorm.batch.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.cluster.DistributedClusterState;
import com.alibaba.jstorm.utils.JStormUtils;

public class BatchCommon {
	private static final Logger LOG = Logger.getLogger(BatchCommon.class);

	private static ClusterState zkClient = null;

	public static ClusterState getZkClient(Map conf) throws Exception {
		synchronized (BatchCommon.class) {
			if (zkClient != null) {
				return zkClient;
			}

			List<String> zkServers = null;
			if (conf.get(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS) != null) {
				zkServers = (List<String>) conf
						.get(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS);
			} else if (conf.get(Config.STORM_ZOOKEEPER_SERVERS) != null) {
				zkServers = (List<String>) conf
						.get(Config.STORM_ZOOKEEPER_SERVERS);
			} else {
				throw new RuntimeException("No setting zk");
			}

			int port = 2181;
			if (conf.get(Config.TRANSACTIONAL_ZOOKEEPER_PORT) != null) {
				port = JStormUtils.parseInt(
						conf.get(Config.TRANSACTIONAL_ZOOKEEPER_PORT), 2181);
			} else if (conf.get(Config.STORM_ZOOKEEPER_PORT) != null) {
				port = JStormUtils.parseInt(
						conf.get(Config.STORM_ZOOKEEPER_PORT), 2181);
			}

			String root = BatchDef.BATCH_ZK_ROOT;
			if (conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT) != null) {
				root = (String) conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT);
			}

			root = root + BatchDef.ZK_SEPERATOR
					+ conf.get(Config.TOPOLOGY_NAME);

			Map<Object, Object> tmpConf = new HashMap<Object, Object>();
			tmpConf.putAll(conf);
			tmpConf.put(Config.STORM_ZOOKEEPER_SERVERS, zkServers);
			tmpConf.put(Config.STORM_ZOOKEEPER_ROOT, root);
			zkClient = new DistributedClusterState(tmpConf);

			LOG.info("Successfully connect ZK");
			return zkClient;
		}

	}
}
