package com.alibaba.jstorm.daemon.supervisor;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.thrift7.TException;

import backtype.storm.utils.LocalState;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.event.EventManager;
import com.alibaba.jstorm.event.EventManagerZkPusher;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.task.LocalAssignment;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;

/**
 * supervisor SynchronizeSupervisor workflow (1) writer local assignment to
 * LocalState (2) download new Assignment's topology (3) remove useless Topology
 * (4) push one SyncProcessEvent to SyncProcessEvent's EventManager
 */
class SyncSupervisorEvent extends RunnableCallback {

	private static final Logger LOG = Logger
			.getLogger(SyncSupervisorEvent.class);

	// private Supervisor supervisor;

	private String supervisorId;

	private EventManager processEventManager;

	private EventManager syncSupEventManager;

	private StormClusterState stormClusterState;

	private LocalState localState;

	private Map<Object, Object> conf;

	private SyncProcessEvent syncProcesses;

	/**
	 * @param conf
	 * @param processEventManager
	 * @param syncSupEventManager
	 * @param stormClusterState
	 * @param supervisorId
	 * @param localState
	 * @param syncProcesses
	 */
	public SyncSupervisorEvent(String supervisorId, Map conf,
			EventManager processEventManager, EventManager syncSupEventManager,
			StormClusterState stormClusterState, LocalState localState,
			SyncProcessEvent syncProcesses) {

		this.syncProcesses = syncProcesses;
		this.processEventManager = processEventManager;
		this.syncSupEventManager = syncSupEventManager;
		this.stormClusterState = stormClusterState;
		this.conf = conf;
		this.supervisorId = supervisorId;
		this.localState = localState;

	}

	@Override
	public void run() {
		LOG.debug("Synchronizing supervisor");

		try {

			RunnableCallback syncCallback = new EventManagerZkPusher(this,
					syncSupEventManager);

			/**
			 * Step 1: get all assignments and register /ZK-dir/assignment and
			 * every assignment watch
			 * 
			 */
			Map<String, Assignment> assignments = Cluster.get_all_assignment(
					stormClusterState, syncCallback);
			LOG.debug("Get all assignments " + assignments);

			/**
			 * Step 2: get topologyIds list from
			 * STORM-LOCAL-DIR/supervisor/stormdist/
			 */
			List<String> downloadedTopologyIds = StormConfig
					.get_supervisor_toplogy_list(conf);
			LOG.debug("Downloaded storm ids: " + downloadedTopologyIds);

			/**
			 * Step 3: get <port,LocalAssignments> from ZK local node's
			 * assignment
			 */
			Map<Integer, LocalAssignment> localAssignment = getLocalAssign(
					stormClusterState, supervisorId, assignments);

			/**
			 * Step 4: writer local assignment to LocalState
			 */
			try {
				LOG.debug("Writing local assignment " + localAssignment);
				localState.put(Common.LS_LOCAL_ASSIGNMENTS, localAssignment);
			} catch (IOException e) {
				LOG.error("put LS_LOCAL_ASSIGNMENTS " + localAssignment
						+ " of localState failed");
				throw e;
			}

			// Step 5: download code from ZK

			Map<String, String> topologyCodes = getTopologyCodeLocations(
					assignments, supervisorId);

			downloadTopology(topologyCodes, downloadedTopologyIds);

			/**
			 * Step 6: remove any downloaded useless topology
			 */
			removeUselessTopology(topologyCodes, downloadedTopologyIds);

			/**
			 * Step 7: push syncProcesses Event
			 */
			processEventManager.add(syncProcesses);

		} catch (Exception e) {
			LOG.error("Failed to Sync Supervisor", e);
			// throw new RuntimeException(e);
		}

	}

	/**
	 * download code ; two cluster mode: local and distributed
	 * 
	 * @param conf
	 * @param topologyId
	 * @param masterCodeDir
	 * @param clusterMode
	 * @throws IOException
	 */
	private void downloadStormCode(Map conf, String topologyId,
			String masterCodeDir) throws IOException, TException {
		String clusterMode = StormConfig.cluster_mode(conf);

		if (clusterMode.endsWith("distributed")) {
			downloadDistributeStormCode(conf, topologyId, masterCodeDir);
		} else if (clusterMode.endsWith("local")) {
			downloadLocalStormCode(conf, topologyId, masterCodeDir);

		}
	}

	private void downloadLocalStormCode(Map conf, String topologyId,
			String masterCodeDir) throws IOException, TException {

		// STORM-LOCAL-DIR/supervisor/stormdist/storm-id
		String stormroot = StormConfig.supervisor_stormdist_root(conf,
				topologyId);

		FileUtils.copyDirectory(new File(masterCodeDir), new File(stormroot));

		ClassLoader classloader = Thread.currentThread()
				.getContextClassLoader();

		String resourcesJar = resourcesJar();

		URL url = classloader.getResource(StormConfig.RESOURCES_SUBDIR);

		String targetDir = stormroot + '/' + StormConfig.RESOURCES_SUBDIR;

		if (resourcesJar != null) {

			LOG.info("Extracting resources from jar at " + resourcesJar
					+ " to " + targetDir);

			JStormUtils.extract_dir_from_jar(resourcesJar,
					StormConfig.RESOURCES_SUBDIR, stormroot);// extract dir
			// from jar;;
			// util.clj
		} else if (url != null) {

			LOG.info("Copying resources at " + url.toString() + " to "
					+ targetDir);

			FileUtils.copyDirectory(new File(url.getFile()), (new File(
					targetDir)));

		}
	}

	/**
	 * Don't need synchronize, due to EventManager will execute serially
	 * 
	 * @param conf
	 * @param topologyId
	 * @param masterCodeDir
	 * @throws IOException
	 * @throws TException
	 */
	private void downloadDistributeStormCode(Map conf, String topologyId,
			String masterCodeDir) throws IOException, TException {

		// STORM_LOCAL_DIR/supervisor/tmp/(UUID)
		String tmproot = StormConfig.supervisorTmpDir(conf) + File.separator
				+ UUID.randomUUID().toString();

		// STORM_LOCAL_DIR/supervisor/stormdist/topologyId
		String stormroot = StormConfig.supervisor_stormdist_root(conf,
				topologyId);

		JStormServerUtils.downloadCodeFromMaster(conf, tmproot, masterCodeDir,
				topologyId, true);

		// tmproot/stormjar.jar
		String localFileJarTmp = StormConfig.stormjar_path(tmproot);

		// extract dir from jar
		JStormUtils.extract_dir_from_jar(localFileJarTmp,
				StormConfig.RESOURCES_SUBDIR, tmproot);

		FileUtils.moveDirectory(new File(tmproot), new File(stormroot));

	}

	private String resourcesJar() {

		String path = System.getProperty("java.class.path");
		if (path == null) {
			return null;
		}

		String[] paths = path.split(File.pathSeparator);

		List<String> jarPaths = new ArrayList<String>();
		for (String s : paths) {
			if (s.endsWith(".jar")) {
				jarPaths.add(s);
			}
		}

		/**
		 * FIXME, this place seems exist problem
		 */
		List<String> rtn = new ArrayList<String>();
		int size = jarPaths.size();
		for (int i = 0; i < size; i++) {
			if (JStormUtils.zipContainsDir(jarPaths.get(i),
					StormConfig.RESOURCES_SUBDIR)) {
				rtn.add(jarPaths.get(i));
			}
		}

		if (rtn.size() == 0)
			return null;

		return rtn.get(0);
	}

	/**
	 * a port must be assigned one topology
	 * 
	 * @param stormClusterState
	 * @param supervisorId
	 * @param callback
	 * @throws Exception
	 * @returns map: {port,LocalAssignment}
	 */
	private Map<Integer, LocalAssignment> getLocalAssign(
			StormClusterState stormClusterState, String supervisorId,
			Map<String, Assignment> assignments) throws Exception {

		Map<Integer, LocalAssignment> portLA = new HashMap<Integer, LocalAssignment>();

		for (Entry<String, Assignment> assignEntry : assignments.entrySet()) {
			String topologyId = assignEntry.getKey();
			Assignment assignment = assignEntry.getValue();

			Map<Integer, LocalAssignment> portTasks = readMyTasks(
					stormClusterState, topologyId, supervisorId, assignment);
			if (portTasks == null) {
				continue;
			}

			// a port must be assigned one storm
			for (Entry<Integer, LocalAssignment> entry : portTasks.entrySet()) {

				Integer port = entry.getKey();

				LocalAssignment la = entry.getValue();

				if (!portLA.containsKey(port)) {
					portLA.put(port, la);
				} else {
					throw new RuntimeException(
							"Should not have multiple topologys assigned to one port");
				}
			}
		}

		return portLA;
	}

	/**
	 * get local node's tasks
	 * 
	 * @param stormClusterState
	 * @param topologyId
	 * @param supervisorId
	 * @param callback
	 * @return Map: {port, LocalAssignment}
	 * @throws Exception
	 */
	private Map<Integer, LocalAssignment> readMyTasks(
			StormClusterState stormClusterState, String topologyId,
			String supervisorId, Assignment assignmenInfo) throws Exception {

		Map<Integer, LocalAssignment> portTasks = new HashMap<Integer, LocalAssignment>();

		StormBase stormBase = stormClusterState.storm_base(topologyId, null);
		if (stormBase == null) {
			LOG.error("Failed to get StormBase of " + topologyId);
			return portTasks;
		}

		Set<ResourceWorkerSlot> workers = assignmenInfo.getWorkers();
		if (workers == null) {
			LOG.error("No worker of assignement's " + assignmenInfo);
			return portTasks;
		}

		for (ResourceWorkerSlot worker : workers) {
			if (!supervisorId.equals(worker.getNodeId()))
				continue;
			portTasks.put(worker.getPort(),
					new LocalAssignment(topologyId, worker.getTasks(),
							stormBase.getStormName(), worker.getMemSize(),
							worker.getCpu(), worker.getJvm()));
		}

		return portTasks;
	}

	/**
	 * get mastercodedir for every topology
	 * 
	 * @param stormClusterState
	 * @param callback
	 * @throws Exception
	 * @returns Map: <topologyId, master-code-dir> from zookeeper
	 */
	public static Map<String, String> getTopologyCodeLocations(
			Map<String, Assignment> assignments, String supervisorId)
			throws Exception {

		Map<String, String> rtn = new HashMap<String, String>();
		for (Entry<String, Assignment> entry : assignments.entrySet()) {
			String topologyid = entry.getKey();
			Assignment assignmenInfo = entry.getValue();

			Set<ResourceWorkerSlot> workers = assignmenInfo.getWorkers();
			for (ResourceWorkerSlot worker : workers) {
				String node = worker.getNodeId();
				if (supervisorId.equals(node)) {
					rtn.put(topologyid, assignmenInfo.getMasterCodeDir());
					break;
				}
			}

		}
		return rtn;
	}

	public void downloadTopology(Map<String, String> topologyCodes,
			List<String> downloadedTopologyIds) throws Exception {

		for (Entry<String, String> entry : topologyCodes.entrySet()) {

			String topologyId = entry.getKey();
			String masterCodeDir = entry.getValue();

			if (!downloadedTopologyIds.contains(topologyId)) {

				LOG.info("Downloading code for storm id " + topologyId
						+ " from " + masterCodeDir);

				try {
					downloadStormCode(conf, topologyId, masterCodeDir);
				} catch (IOException e) {
					LOG.error(e + " downloadStormCode failed " + "topologyId:"
							+ topologyId + "masterCodeDir:" + masterCodeDir);
					throw e;

				} catch (TException e) {
					LOG.error(e + " downloadStormCode failed " + "topologyId:"
							+ topologyId + "masterCodeDir:" + masterCodeDir);
					throw e;
				}
				LOG.info("Finished downloading code for storm id " + topologyId
						+ " from " + masterCodeDir);
			}

		}
	}

	public void removeUselessTopology(Map<String, String> topologyCodes,
			List<String> downloadedTopologyIds) {
		for (String topologyId : downloadedTopologyIds) {

			if (!topologyCodes.containsKey(topologyId)) {

				LOG.info("Removing code for storm id " + topologyId);

				String path = null;
				try {
					path = StormConfig.supervisor_stormdist_root(conf,
							topologyId);
					PathUtils.rmr(path);
				} catch (IOException e) {
					String errMsg = "rmr the path:" + path + "failed\n";
					LOG.error(errMsg, e);
				}
			}
		}
	}

}
