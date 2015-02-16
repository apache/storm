package backtype.storm;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.messaging.IContext;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.nimbus.DefaultInimbus;
import com.alibaba.jstorm.daemon.nimbus.NimbusServer;
import com.alibaba.jstorm.daemon.supervisor.Supervisor;
import com.alibaba.jstorm.message.netty.NettyContext;
import com.alibaba.jstorm.zk.Factory;
import com.alibaba.jstorm.zk.Zookeeper;

public class LocalUtils {

	public static Logger LOG = Logger.getLogger(LocalUtils.class);

	public static LocalClusterMap prepareLocalCluster() {
		LocalClusterMap state = new LocalClusterMap();
		try {
			List<String> tmpDirs = new ArrayList();

			String zkDir = getTmpDir();
			tmpDirs.add(zkDir);
			Factory zookeeper = startLocalZookeeper(zkDir);
			Map conf = getLocalConf(zookeeper.getZooKeeperServer()
					.getClientPort());

			String nimbusDir = getTmpDir();
			tmpDirs.add(nimbusDir);
			Map nimbusConf = deepCopyMap(conf);
			nimbusConf.put(Config.STORM_LOCAL_DIR, nimbusDir);
			NimbusServer instance = new NimbusServer();

			Map supervisorConf = deepCopyMap(conf);
			String supervisorDir = getTmpDir();
			tmpDirs.add(supervisorDir);
			supervisorConf.put(Config.STORM_LOCAL_DIR, supervisorDir);
			Supervisor supervisor = new Supervisor();
			IContext context = getLocalContext(supervisorConf);

			state.setNimbusServer(instance);
			state.setNimbus(instance.launcherLocalServer(nimbusConf,
					new DefaultInimbus()));
			state.setZookeeper(zookeeper);
			state.setConf(conf);
			state.setTmpDir(tmpDirs);
			state.setSupervisor(supervisor
					.mkSupervisor(supervisorConf, context));
			return state;
		} catch (Exception e) {
			LOG.error("prepare cluster error!", e);
			state.clean();

		}
		return null;
	}

	private static Factory startLocalZookeeper(String tmpDir) {
		for (int i = 2000; i < 65535; i++) {
			try {
				return Zookeeper.mkInprocessZookeeper(tmpDir, i);
			} catch (Exception e) {
				LOG.error("fail to launch zookeeper at port: " + i, e);
			}
		}
		throw new RuntimeException(
				"No port is available to launch an inprocess zookeeper.");
	}

	private static String getTmpDir() {
		return System.getProperty("java.io.tmpdir") + File.separator
				+ UUID.randomUUID();
	}

	private static Map getLocalConf(int port) {
		List<String> zkServers = new ArrayList<String>(1);
		zkServers.add("localhost");
		Map conf = Utils.readStormConfig();
		conf.put(Config.STORM_CLUSTER_MODE, "local");
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, zkServers);
		conf.put(Config.STORM_ZOOKEEPER_PORT, port);
		conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
		conf.put(Config.ZMQ_LINGER_MILLIS, 0);
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, false);
		conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 50);
		ConfigExtension.setSpoutDelayRunSeconds(conf, 0);
		ConfigExtension.setTaskCleanupTimeoutSec(conf, 0);
		return conf;
	}

	private static IContext getLocalContext(Map conf) {
		if (!(Boolean) conf.get(Config.STORM_LOCAL_MODE_ZMQ)) {
			IContext result = new NettyContext();
			result.prepare(conf);
			return result;
		}
		return null;
	}

	private static Map deepCopyMap(Map map) {
		return new HashMap(map);
	}
}
