package com.alibaba.jstorm.cluster;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.utils.PathUtils;

public class StormConfig {
	private final static Logger LOG = Logger.getLogger(StormConfig.class);
	public final static String RESOURCES_SUBDIR = "resources";
	public final static String WORKER_DATA_SUBDIR = "worker_shared_data";

	public static final String FILE_SEPERATEOR = File.separator;

	public static String clojureConfigName(String name) {
		return name.toUpperCase().replace("_", "-");
	}

	public static Map read_storm_config() {
		return Utils.readStormConfig();
	}

	public static Map read_yaml_config(String name) {
		return Utils.findAndReadConfigFile(name, true);
	}

	public static Map read_default_config() {
		return Utils.readDefaultConfig();
	}

	public static List<Object> All_CONFIGS() {
		List<Object> rtn = new ArrayList<Object>();
		Config config = new Config();
		Class<?> ConfigClass = config.getClass();
		Field[] fields = ConfigClass.getFields();
		for (int i = 0; i < fields.length; i++) {
			try {
				Object obj = fields[i].get(null);
				rtn.add(obj);
			} catch (IllegalArgumentException e) {
				LOG.error(e.getMessage(), e);
			} catch (IllegalAccessException e) {
				LOG.error(e.getMessage(), e);
			}
		}
		return rtn;
	}

	public static HashMap<String, Object> getClassFields(Class<?> cls)
			throws IllegalArgumentException, IllegalAccessException {
		java.lang.reflect.Field[] list = cls.getDeclaredFields();
		HashMap<String, Object> rtn = new HashMap<String, Object>();
		for (java.lang.reflect.Field f : list) {
			String name = f.getName();
			rtn.put(name, f.get(null).toString());

		}
		return rtn;
	}

	public static String cluster_mode(Map conf) {
		String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
		return mode;

	}

	public static boolean local_mode(Map conf) {
		String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
		if (mode != null) {
			if (mode.equals("local")) {
				return true;
			}

			if (mode.equals("distributed")) {
				return false;
			}
		}
		throw new IllegalArgumentException("Illegal cluster mode in conf:"
				+ mode);

	}

	/**
	 * validate whether the mode is distributed
	 * 
	 * @param conf
	 */
	public static void validate_distributed_mode(Map<?, ?> conf) {
		if (StormConfig.local_mode(conf)) {
			throw new IllegalArgumentException(
					"Cannot start server in local mode!");
		}

	}
	
	public static void validate_local_mode(Map<?, ?> conf) {
		if (!StormConfig.local_mode(conf)) {
			throw new IllegalArgumentException(
					"Cannot start server in distributed mode!");
		}

	}

	public static String worker_root(Map conf) throws IOException {
		String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR))
				+ FILE_SEPERATEOR + "workers";
		FileUtils.forceMkdir(new File(ret));
		return ret;
	}

	public static String worker_root(Map conf, String id) throws IOException {
		String ret = worker_root(conf) + FILE_SEPERATEOR + id;
		FileUtils.forceMkdir(new File(ret));
		return ret;
	}

	public static String worker_pids_root(Map conf, String id)
			throws IOException {
		String ret = worker_root(conf, id) + FILE_SEPERATEOR + "pids";
		FileUtils.forceMkdir(new File(ret));
		return ret;
	}

	public static String worker_pid_path(Map conf, String id, String pid)
			throws IOException {
		String ret = worker_pids_root(conf, id) + FILE_SEPERATEOR + pid;
		return ret;
	}

	public static String worker_heartbeats_root(Map conf, String id)
			throws IOException {
		String ret = worker_root(conf, id) + FILE_SEPERATEOR + "heartbeats";
		FileUtils.forceMkdir(new File(ret));
		return ret;
	}

	public static String default_worker_shared_dir(Map conf) throws IOException {
		String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR))
				+ FILE_SEPERATEOR + WORKER_DATA_SUBDIR;

		FileUtils.forceMkdir(new File(ret));
		return ret;
	}

	private static String supervisor_local_dir(Map conf) throws IOException {
		String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR))
				+ FILE_SEPERATEOR + "supervisor";
		FileUtils.forceMkdir(new File(ret));
		return ret;
	}

	public static String supervisor_stormdist_root(Map conf) throws IOException {
		String ret = stormdist_path(supervisor_local_dir(conf));
		FileUtils.forceMkdir(new File(ret));
		return ret;
	}

	public static String supervisor_stormdist_root(Map conf, String topologyId)
			throws IOException {
		return supervisor_stormdist_root(conf) + FILE_SEPERATEOR + topologyId;
	}
	
	/**
	 * Return supervisor's pid dir
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static String supervisorPids(Map conf)throws IOException {
		String ret = supervisor_local_dir(conf) + FILE_SEPERATEOR + "pids";
		try {
			FileUtils.forceMkdir(new File(ret));
		} catch (IOException e) {
			LOG.error("Failed to create dir " + ret, e);
			throw e;
		}
		return ret;
	}
	
	
	/**
	 * Return nimbus's heartbeat dir for apsara
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static String supervisorHearbeatForContainer(Map conf)throws IOException {
		String ret = supervisor_local_dir(conf) + FILE_SEPERATEOR + "supervisor.heartbeat";
		try {
			FileUtils.forceMkdir(new File(ret));
		} catch (IOException e) {
			LOG.error("Failed to create dir " + ret, e);
			throw e;
		}
		return ret;
	}

	public static String stormjar_path(String stormroot) {
		return stormroot + FILE_SEPERATEOR + "stormjar.jar";
	}

	public static String stormcode_path(String stormroot) {
		return stormroot + FILE_SEPERATEOR + "stormcode.ser";
	}

	public static String stormconf_path(String stormroot) {
		return stormroot + FILE_SEPERATEOR + "stormconf.ser";
	}
	
	public static String stormlib_path(String stormroot, String libname) {
		return stormroot + FILE_SEPERATEOR + "lib" + FILE_SEPERATEOR + libname;
	}
	
	public static String stormlib_path(String stormroot) {
		return stormroot + FILE_SEPERATEOR + "lib";
	}

	public static String stormdist_path(String stormroot) {
		return stormroot + FILE_SEPERATEOR + "stormdist";
	}

	public static String supervisor_storm_resources_path(String stormroot) {
		return stormroot + FILE_SEPERATEOR + RESOURCES_SUBDIR;
	}
	
	public static String stormtmp_path(String stormroot) {
		return stormroot + FILE_SEPERATEOR + "tmp";
	}

	public static LocalState worker_state(Map conf, String id)
			throws IOException {
		String path = worker_heartbeats_root(conf, id);

		LocalState rtn = new LocalState(path);
		return rtn;

	}

	public static String masterLocalDir(Map conf) throws IOException {
		String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR))
				+ FILE_SEPERATEOR + "nimbus";
		try {
			FileUtils.forceMkdir(new File(ret));
		} catch (IOException e) {
			LOG.error("Failed to create dir " + ret, e);
			throw e;
		}
		return ret;
	}

	public static String masterStormdistRoot(Map conf) throws IOException {
		String ret = stormdist_path(masterLocalDir(conf));
		FileUtils.forceMkdir(new File(ret));
		return ret;
	}

	public static String masterStormdistRoot(Map conf, String topologyId)
			throws IOException {
		return masterStormdistRoot(conf) + FILE_SEPERATEOR + topologyId;
	}

	public static String masterStormTmpRoot(Map conf) throws IOException {
		String ret = stormtmp_path(masterLocalDir(conf));
		FileUtils.forceMkdir(new File(ret));
		return ret;
	}

	public static String masterStormTmpRoot(Map conf, String topologyId)
			throws IOException {
		return masterStormTmpRoot(conf) + FILE_SEPERATEOR + topologyId;
	}
	
	public static String masterInbox(Map conf) throws IOException {
		String ret = masterLocalDir(conf) + FILE_SEPERATEOR + "inbox";
		try {
			FileUtils.forceMkdir(new File(ret));
		} catch (IOException e) {
			LOG.error("Failed to create dir " + ret, e);
			throw e;
		}
		return ret;
	}

	public static String masterInimbus(Map conf) throws IOException {
		String ret = masterLocalDir(conf) + FILE_SEPERATEOR + "ininumbus";
		try {
			FileUtils.forceMkdir(new File(ret));
		} catch (IOException e) {
			LOG.error("Failed to create dir " + ret, e);
			throw e;
		}
		return ret;
	}
	
	/**
	 * Return nimbus's pid dir
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static String masterPids(Map conf)throws IOException {
		String ret = masterLocalDir(conf) + FILE_SEPERATEOR + "pids";
		try {
			FileUtils.forceMkdir(new File(ret));
		} catch (IOException e) {
			LOG.error("Failed to create dir " + ret, e);
			throw e;
		}
		return ret;
	}
	
	
	/**
	 * Return nimbus's heartbeat dir for apsara
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static String masterHearbeatForContainer(Map conf)throws IOException {
		String ret = masterLocalDir(conf) + FILE_SEPERATEOR + "nimbus.heartbeat";
		try {
			FileUtils.forceMkdir(new File(ret));
		} catch (IOException e) {
			LOG.error("Failed to create dir " + ret, e);
			throw e;
		}
		return ret;
	}

	public static String supervisorTmpDir(Map conf) throws IOException {
		String ret = null;
		try {
			ret = supervisor_local_dir(conf) + FILE_SEPERATEOR + "tmp";
			FileUtils.forceMkdir(new File(ret));
		} catch (IOException e) {
			LOG.error("Failed to create dir " + ret, e);
			throw e;

		}

		return ret;
	}

	public static LocalState supervisorState(Map conf) throws IOException {
		LocalState localState = null;
		try {
			String localstateDir = supervisor_local_dir(conf) + FILE_SEPERATEOR
					+ "localstate";
			FileUtils.forceMkdir(new File(localstateDir));
			localState = new LocalState(localstateDir);
		} catch (IOException e) {
			LOG.error("Failed to create supervisor LocalState", e);
			throw e;
		}
		return localState;
	}

	/**
	 * stormconf is mergered into clusterconf
	 * 
	 * @param conf
	 * @param topologyId
	 * @return
	 * @throws IOException
	 */
	public static Map read_supervisor_topology_conf(Map conf, String topologyId)
			throws IOException {
		String topologyRoot = StormConfig.supervisor_stormdist_root(conf,
				topologyId);
		String confPath = StormConfig.stormconf_path(topologyRoot);
		return (Map) readLocalObject(topologyId, confPath);
	}

	public static StormTopology read_supervisor_topology_code(Map conf,
			String topologyId) throws IOException {
		String topologyRoot = StormConfig.supervisor_stormdist_root(conf,
				topologyId);
		String codePath = StormConfig.stormcode_path(topologyRoot);
		return (StormTopology) readLocalObject(topologyId, codePath);
	}

	@SuppressWarnings("rawtypes")
	public static List<String> get_supervisor_toplogy_list(Map conf)
			throws IOException {

		// get the path: STORM-LOCAL-DIR/supervisor/stormdist/
		String path = StormConfig.supervisor_stormdist_root(conf);

		List<String> topologyids = PathUtils.read_dir_contents(path);

		return topologyids;
	}

	public static Map read_nimbus_topology_conf(Map conf, String topologyId)
			throws IOException {
		String topologyRoot = StormConfig.masterStormdistRoot(conf, topologyId);
		return read_topology_conf(topologyRoot, topologyId);
	}
	
	public static Map read_nimbusTmp_topology_conf(Map conf, String topologyId)
	       throws IOException {
		String topologyRoot = StormConfig.masterStormTmpRoot(conf, topologyId);
		return read_topology_conf(topologyRoot, topologyId);
	}
	
	public static Map read_topology_conf(String topologyRoot, String topologyId)
	       throws IOException {
		String readFile = StormConfig.stormconf_path(topologyRoot);
		return (Map) readLocalObject(topologyId, readFile);
	}

	public static StormTopology read_nimbus_topology_code(Map conf,
			String topologyId) throws IOException {
		String topologyRoot = StormConfig.masterStormdistRoot(conf, topologyId);
		String codePath = StormConfig.stormcode_path(topologyRoot);
		return (StormTopology) readLocalObject(topologyId, codePath);
	}

	/**
	 * stormconf has mergered into clusterconf
	 * 
	 * @param conf
	 * @param topologyId
	 * @return Map
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static Object readLocalObject(String topologyId, String readFile)
			throws IOException {

		String errMsg = "Failed to get topology configuration of " + topologyId
				+ " file:" + readFile;

		byte[] bconf = FileUtils.readFileToByteArray(new File(readFile));
		if (bconf == null) {
			errMsg += ", due to failed to read";
			LOG.error(errMsg);
			throw new IOException(errMsg);
		}

		Object ret = null;
		try {
			ret = Utils.deserialize(bconf);
		} catch (Exception e) {
			errMsg += ", due to failed to serialized the data";
			LOG.error(errMsg);
			throw new IOException(errMsg);
		}

		return ret;
	}

	public static Integer sampling_rate(Map conf) {
		return (int) (1 / Double.parseDouble(String.valueOf(conf
				.get(Config.TOPOLOGY_STATS_SAMPLE_RATE))));
	}
	

}
