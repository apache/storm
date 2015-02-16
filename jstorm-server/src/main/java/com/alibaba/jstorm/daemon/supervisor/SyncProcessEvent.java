package com.alibaba.jstorm.daemon.supervisor;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.GenericOptionsParser;
import backtype.storm.messaging.IContext;
import backtype.storm.utils.LocalState;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.ProcessSimulator;
import com.alibaba.jstorm.daemon.worker.State;
import com.alibaba.jstorm.daemon.worker.Worker;
import com.alibaba.jstorm.daemon.worker.WorkerHeartbeat;
import com.alibaba.jstorm.daemon.worker.WorkerShutdown;
import com.alibaba.jstorm.message.zeroMq.MQContext;
import com.alibaba.jstorm.task.LocalAssignment;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.utils.PathUtils;
import com.alibaba.jstorm.utils.TimeFormat;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * SyncProcesses (1) kill bad worker (2) start new worker
 */
class SyncProcessEvent extends ShutdownWork {
	private static Logger LOG = Logger.getLogger(SyncProcessEvent.class);

	private LocalState localState;

	private Map conf;

	private ConcurrentHashMap<String, String> workerThreadPids;

	private String supervisorId;

	private IContext sharedContext;

	private CgroupManager cgroupManager;

	private SandBoxMaker sandBoxMaker;

	/**
	 * Due to the worker startTime is put in Supervisor memory,
	 * When supervisor restart, the starting worker is likely to be killed
	 */
	private Map<String, Pair<Integer, Integer>> workerIdToStartTimeAndPort;

	// private Supervisor supervisor;

	/**
	 * @param conf
	 * @param localState
	 * @param workerThreadPids
	 * @param supervisorId
	 * @param sharedContext
	 * @param workerThreadPidsReadLock
	 * @param workerThreadPidsWriteLock
	 */
	public SyncProcessEvent(String supervisorId, Map conf,
			LocalState localState,
			ConcurrentHashMap<String, String> workerThreadPids,
			IContext sharedContext) {

		this.supervisorId = supervisorId;

		this.conf = conf;

		this.localState = localState;

		this.workerThreadPids = workerThreadPids;

		// right now, sharedContext is null
		this.sharedContext = sharedContext;

		this.sandBoxMaker = new SandBoxMaker(conf);

		this.workerIdToStartTimeAndPort = new HashMap<String, Pair<Integer, Integer>>();

		if (ConfigExtension.isEnableCgroup(conf)) {
			cgroupManager = new CgroupManager(conf);
		}
	}

	/**
	 * @@@ Change the old logic In the old logic, it will store
	 *     LS_LOCAL_ASSIGNMENTS Map<String, Integer> into LocalState
	 * 
	 *     But I don't think LS_LOCAL_ASSIGNMENTS is useful, so remove this
	 *     logic
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		LOG.debug("Syncing processes");
		try {

			/**
			 * Step 1: get assigned tasks from localstat Map<port(type Integer),
			 * LocalAssignment>
			 */
			Map<Integer, LocalAssignment> localAssignments = null;
			try {
				localAssignments = (Map<Integer, LocalAssignment>) localState
						.get(Common.LS_LOCAL_ASSIGNMENTS);
			} catch (IOException e) {
				LOG.error("Failed to get LOCAL_ASSIGNMENTS from LocalState", e);
				throw e;
			}

			if (localAssignments == null) {
				localAssignments = new HashMap<Integer, LocalAssignment>();
			}
			LOG.debug("Assigned tasks: " + localAssignments);

			/**
			 * Step 2: get local WorkerStats from local_dir/worker/ids/heartbeat
			 * Map<workerid [WorkerHeartbeat, state]>
			 */
			Map<String, StateHeartbeat> localWorkerStats = null;
			try {
				localWorkerStats = getLocalWorkerStats(conf, localState,
						localAssignments);
			} catch (Exception e) {
				LOG.error("Failed to get Local worker stats");
				throw e;
			}
			LOG.debug("Allocated: " + localWorkerStats);

			/**
			 * Step 3: kill Invalid Workers and remove killed worker from
			 * localWorkerStats
			 */
			Set<Integer> keepPorts = killUselessWorkers(localWorkerStats);

			// check new workers
			checkNewWorkers(conf);

			// start new workers
			startNewWorkers(keepPorts, localAssignments);

		} catch (Exception e) {
			LOG.error("Failed Sync Process", e);
			// throw e
		}

	}

	/**
	 * mark all new Workers
	 * 
	 * @param workerIds
	 * @pdOid 52b11418-7474-446d-bff5-0ecd68f4954f
	 */
	public void markAllNewWorkers(Map<Integer, String> workerIds) {

		int startTime = TimeUtils.current_time_secs();

		for (Entry<Integer, String> entry : workerIds.entrySet()) {

			workerIdToStartTimeAndPort.put(entry.getValue(),
					new Pair<Integer, Integer>(startTime, entry.getKey()));

		}
	}

	/**
	 * check new workers if the time is not > *
	 * SUPERVISOR_WORKER_START_TIMEOUT_SECS, otherwise info failed
	 * 
	 * @param conf
	 * @pdOid f0a6ab43-8cd3-44e1-8fd3-015a2ec51c6a
	 */
	public void checkNewWorkers(Map conf) throws IOException,
			InterruptedException {

		Set<String> workers = new HashSet<String>();
		for (Entry<String, Pair<Integer, Integer>> entry : workerIdToStartTimeAndPort
				.entrySet()) {
			String workerId = entry.getKey();
			int startTime = entry.getValue().getFirst();
			LocalState ls = StormConfig.worker_state(conf, workerId);
			WorkerHeartbeat whb = (WorkerHeartbeat) ls
					.get(Common.LS_WORKER_HEARTBEAT);
			if (whb == null) {
				if ((TimeUtils.current_time_secs() - startTime) < JStormUtils
						.parseInt(conf
								.get(Config.SUPERVISOR_WORKER_START_TIMEOUT_SECS))) {
					LOG.info(workerId + " still hasn't started");
				} else {
					LOG.error("Failed to start Worker " + workerId);
					workers.add(workerId);
				}
			} else {
				LOG.info("Successfully start worker " + workerId);
				workers.add(workerId);
			}
		}
		for (String workerId : workers) {
			this.workerIdToStartTimeAndPort.remove(workerId);
		}
	}

	/**
	 * get localstat approved workerId's map
	 * 
	 * @return Map<workerid [workerheart, state]> [workerheart, state] is also a
	 *         map, key is "workheartbeat" and "state"
	 * @param conf
	 * @param localState
	 * @param assignedTasks
	 * @throws IOException
	 * @pdOid 11c9bebb-d082-4c51-b323-dd3d5522a649
	 */
	@SuppressWarnings("unchecked")
	public Map<String, StateHeartbeat> getLocalWorkerStats(Map conf,
			LocalState localState, Map<Integer, LocalAssignment> assignedTasks)
			throws Exception {

		Map<String, StateHeartbeat> workeridHbstate = new HashMap<String, StateHeartbeat>();

		int now = TimeUtils.current_time_secs();

		/**
		 * Get Map<workerId, WorkerHeartbeat> from
		 * local_dir/worker/ids/heartbeat
		 */
		Map<String, WorkerHeartbeat> idToHeartbeat = readWorkerHeartbeats(conf);
		for (Map.Entry<String, WorkerHeartbeat> entry : idToHeartbeat
				.entrySet()) {

			String workerid = entry.getKey().toString();

			WorkerHeartbeat whb = entry.getValue();

			State state = null;

			if (whb == null) {

				state = State.notStarted;

			} else if (matchesAssignment(whb, assignedTasks) == false) {

				// workerId isn't approved or
				// isn't assigned task
				state = State.disallowed;

			} else if ((now - whb.getTimeSecs()) > JStormUtils.parseInt(conf
					.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS))) {//

				state = State.timedOut;
			} else {
				if (isWorkerDead(workerid)) {
					state = State.timedOut;
				}else {
					state = State.valid;
				}
			}

			if (state != State.valid) {
				LOG.info("Worker:" + workerid + " state:" + state
						+ " WorkerHeartbeat: " + whb
						+ " at supervisor time-secs " + now);
			} else {
				LOG.debug("Worker:" + workerid + " state:" + state
						+ " WorkerHeartbeat: " + whb
						+ " at supervisor time-secs " + now);
			}

			workeridHbstate.put(workerid, new StateHeartbeat(state, whb));
		}

		return workeridHbstate;
	}

	/**
	 * check whether the workerheartbeat is allowed in the assignedTasks
	 * 
	 * @param whb
	 *            : WorkerHeartbeat
	 * @param assignedTasks
	 * @return boolean if true, the assignments(LS-LOCAL-ASSIGNMENTS) is match
	 *         with workerheart if fasle, is not matched
	 */
	public boolean matchesAssignment(WorkerHeartbeat whb,
			Map<Integer, LocalAssignment> assignedTasks) {

		boolean isMatch = true;
		LocalAssignment localAssignment = assignedTasks.get(whb.getPort());

		if (localAssignment == null) {
			isMatch = false;
		} else if (!whb.getTopologyId().equals(localAssignment.getTopologyId())) {
			// topology id not equal
			LOG.info("topology id not equal whb=" + whb.getTopologyId()
					+ ",localAssignment=" + localAssignment.getTopologyId());
			isMatch = false;
		} else if (!(whb.getTaskIds().equals(localAssignment.getTaskIds()))) {
			// task-id isn't equal
			LOG.info("task-id isn't equal whb=" + whb.getTaskIds()
					+ ",localAssignment=" + localAssignment.getTaskIds());
			isMatch = false;
		}

		return isMatch;
	}

	/**
	 * get all workers heartbeats of the supervisor
	 * 
	 * @param conf
	 * @return Map<workerId, WorkerHeartbeat>
	 * @throws IOException
	 * @throws IOException
	 */
	public Map<String, WorkerHeartbeat> readWorkerHeartbeats(Map conf)
			throws Exception {

		Map<String, WorkerHeartbeat> workerHeartbeats = new HashMap<String, WorkerHeartbeat>();

		// get the path: STORM-LOCAL-DIR/workers
		String path = StormConfig.worker_root(conf);

		List<String> workerIds = PathUtils.read_dir_contents(path);

		if (workerIds == null) {
			LOG.info("No worker dir under " + path);
			return workerHeartbeats;

		}

		for (String workerId : workerIds) {

			WorkerHeartbeat whb = readWorkerHeartbeat(conf, workerId);

			// ATTENTION: whb can be null
			workerHeartbeats.put(workerId, whb);
		}
		return workerHeartbeats;
	}

	/**
	 * get worker heartbeat by workerid
	 * 
	 * @param conf
	 * @param workerId
	 * @returns WorkerHeartbeat
	 * @throws IOException
	 */
	public WorkerHeartbeat readWorkerHeartbeat(Map conf, String workerId)
			throws Exception {

		try {
			LocalState ls = StormConfig.worker_state(conf, workerId);

			return (WorkerHeartbeat) ls.get(Common.LS_WORKER_HEARTBEAT);
		} catch (IOException e) {
			LOG.error("Failed to get worker Heartbeat", e);
			return null;
		}

	}

	/**
	 * launch a worker in local mode
	 * 
	 * @param conf
	 * @param sharedcontext
	 * @param topologyId
	 * @param supervisorId
	 * @param port
	 * @param workerId
	 * @param workerThreadPidsAtom
	 * @param workerThreadPidsAtomWriteLock
	 * @pdOid 405f44c7-bc1b-4e16-85cc-b59352b6ff5d
	 */
	public void launchWorker(Map conf, IContext sharedcontext,
			String topologyId, String supervisorId, Integer port,
			String workerId,
			ConcurrentHashMap<String, String> workerThreadPidsAtom)
			throws Exception {

		String pid = UUID.randomUUID().toString();

		WorkerShutdown worker = Worker.mk_worker(conf, sharedcontext,
				topologyId, supervisorId, port, workerId, null);

		ProcessSimulator.registerProcess(pid, worker);

		workerThreadPidsAtom.put(workerId, pid);

	}
	
	// filter conflict jar
	private Set<String> setFilterJars(Map totalConf) {
		Set<String> filterJars = new HashSet<String>();
		
		boolean enableClassloader = ConfigExtension.isEnableTopologyClassLoader(totalConf);
		if (enableClassloader == false) {
			//avoid logback vs log4j conflict
			boolean enableLogback = false;
			String userDefLogbackConf = ConfigExtension.getUserDefinedLogbackConf(totalConf);
			if (StringUtils.isBlank(userDefLogbackConf) == false) {
				enableLogback = true;
			}
			
			if (enableLogback == true) {
				filterJars.add("slf4j-log4j");
				filterJars.add("log4j");
				
			}else {
				filterJars.add("log4j-over-slf4j");
			}
		}

		LOG.info("Remove jars " + filterJars);
		return filterJars;
	}

	public static boolean isKeyContain(Collection<String> collection, String jar) {
		if (collection == null) {
			return false;
		}
		File file = new File(jar);
		String fileName = file.getName();
		for(String item : collection) {
			
			if (fileName.startsWith(item)) {
				return true;
			}
		}
		return false;
	}
	
	private String getClassPath(String stormjar, String stormHome, Map totalConf) {

		// String classpath = JStormUtils.current_classpath() + ":" + stormjar;
		// return classpath;

		String classpath = JStormUtils.current_classpath();

		String[] classpathes = classpath.split(":");

		Set<String> classSet = new HashSet<String>();

		for (String classJar : classpathes) {
			if (StringUtils.isBlank(classJar) == true) {
				continue;
			}
			classSet.add(classJar);
		}

		if (stormHome != null) {
			List<String> stormHomeFiles = PathUtils
					.read_dir_contents(stormHome);

			for (String file : stormHomeFiles) {
				if (file.endsWith(".jar")) {
					classSet.add(stormHome + File.separator + file);
				}
			}

			List<String> stormLibFiles = PathUtils.read_dir_contents(stormHome
					+ File.separator + "lib");
			for (String file : stormLibFiles) {
				if (file.endsWith(".jar")) {
					classSet.add(stormHome + File.separator + "lib"
							+ File.separator + file);
				}
			}

		}

		Set<String> filterJars = setFilterJars(totalConf);

		StringBuilder sb = new StringBuilder();
		for (String jar : classSet) {
			if (isKeyContain(filterJars, jar)) {
				LOG.info("Remove " + jar);
				continue;
			}
			sb.append(jar + ":");
		}

		if (ConfigExtension.isEnableTopologyClassLoader(totalConf)) {
			return sb.toString().substring(0, sb.length() - 1);
		} else {
			sb.append(stormjar);
			return sb.toString();
		}

	}

	public String getChildOpts(Map stormConf) {
		String childopts = " ";

		if (stormConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS) != null) {
			childopts += (String) stormConf
					.get(Config.TOPOLOGY_WORKER_CHILDOPTS);
		} else if (ConfigExtension.getWorkerGc(stormConf) != null) {
			childopts += ConfigExtension.getWorkerGc(stormConf);
		}

		return childopts;
	}
	
	public String getLogParameter(Map conf, String stormHome, String topologyName, int port) {
		String logFileName = JStormUtils.genLogName(
				topologyName, port);
		// String logFileName = topologyId + "-worker-" + port + ".log";
		
		StringBuilder commandSB = new StringBuilder();
		commandSB.append(" -Dlogfile.name=");
		commandSB.append(logFileName);
		// commandSB.append(" -Dlog4j.ignoreTCL=true");
		
		String userDefLogConf = ConfigExtension.getUserDefinedLog4jConf(conf);
		
		String logConf = System.getProperty("log4j.configuration");
		
		if (StringUtils.isBlank(userDefLogConf) == false) {
			LOG.info("Use user fined log4j conf " + userDefLogConf);
			commandSB.append(" -Dlog4j.configuration=").append(userDefLogConf);
		}else if (StringUtils.isBlank(logConf) == false) {
			commandSB.append(" -Dlog4j.configuration=").append(logConf);
		}else if (StringUtils.isBlank(stormHome) == false){
			commandSB.append(" -Dlog4j.configuration=File:")
				.append(stormHome)
				.append(File.separator).append("conf").append(File.separator)
				.append("jstorm.log4j.properties");
		}else {
			commandSB.append(" -Dlog4j.configuration=File:jstorm.log4j.properties");
		}
		
		String userDefLogbackConf = ConfigExtension.getUserDefinedLogbackConf(conf);
		if (StringUtils.isBlank(userDefLogbackConf) == false) {
			commandSB.append(" -Dlogback.configurationFile=").append(userDefLogbackConf);
		}
		
		return commandSB.toString();
	}

	private String getGcDumpParam(Map totalConf) {
		// String gcPath = ConfigExtension.getWorkerGcPath(totalConf);
		String gcPath = JStormUtils.getLogDir();

		Date now = new Date();
		String nowStr = TimeFormat.getSecond(now);

		StringBuilder gc = new StringBuilder();

		gc.append(" -Xloggc:");
		gc.append(gcPath);
		gc.append(File.separator);
		gc.append("%TOPOLOGYID%-worker-%ID%-");
		gc.append(nowStr);
		gc.append("-gc.log -verbose:gc -XX:HeapDumpPath=");
		gc.append(gcPath).append(File.separator).append("java-%TOPOLOGYID%-")
			.append(nowStr).append(".hprof");
		gc.append(" ");

		return gc.toString();
	}

	/**
	 * launch a worker in distributed mode
	 * 
	 * @param conf
	 * @param sharedcontext
	 * @param topologyId
	 * @param supervisorId
	 * @param port
	 * @param workerId
	 * @throws IOException
	 * @pdOid 6ea369dd-5ce2-4212-864b-1f8b2ed94abb
	 */
	public void launchWorker(Map conf, IContext sharedcontext,
			String topologyId, String supervisorId, Integer port,
			String workerId, LocalAssignment assignment) throws IOException {

		// STORM-LOCAL-DIR/supervisor/stormdist/topologyId
		String stormroot = StormConfig.supervisor_stormdist_root(conf,
				topologyId);

		// STORM-LOCAL-DIR/supervisor/stormdist/topologyId/stormjar.jar
		String stormjar = StormConfig.stormjar_path(stormroot);

		// get supervisor conf
		Map stormConf = StormConfig.read_supervisor_topology_conf(conf,
				topologyId);

		Map totalConf = new HashMap();
		totalConf.putAll(conf);
		totalConf.putAll(stormConf);

		// get classpath
		// String[] param = new String[1];
		// param[0] = stormjar;
		// String classpath = JStormUtils.add_to_classpath(
		// JStormUtils.current_classpath(), param);

		// get child process parameter

		String stormhome = System.getProperty("jstorm.home");

		long memSize = assignment.getMem();
		int cpuNum = assignment.getCpu();
		String childopts = getChildOpts(totalConf);

		childopts += getGcDumpParam(totalConf);

		Map<String, String> environment = new HashMap<String, String>();

		if (ConfigExtension.getWorkerRedirectOutput(totalConf)) {
			environment.put("REDIRECT", "true");
		} else {
			environment.put("REDIRECT", "false");
		}

		environment.put("LD_LIBRARY_PATH",
				(String) totalConf.get(Config.JAVA_LIBRARY_PATH));

		StringBuilder commandSB = new StringBuilder();

		try {
			if (this.cgroupManager != null) {
				commandSB
						.append(cgroupManager.startNewWorker(cpuNum, workerId));
			}
		} catch (Exception e) {
			LOG.error("fail to prepare cgroup to workerId: " + workerId, e);
			return;
		}

		// commandSB.append("java -server -Xdebug -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n ");
		commandSB.append("java -server ");
		commandSB.append(" -Xms" + memSize);
		commandSB.append(" -Xmx" + memSize + " ");
		commandSB.append(" -Xmn" + memSize / 3 + " ");
		commandSB.append(" -XX:PermSize=" + memSize / 16);
		commandSB.append(" -XX:MaxPermSize=" + memSize / 8);
		commandSB.append(" " + childopts);
		commandSB.append(" "
				+ (assignment.getJvm() == null ? "" : assignment.getJvm()));

		commandSB.append(" -Djava.library.path=");
		commandSB.append((String) totalConf.get(Config.JAVA_LIBRARY_PATH));

		if (stormhome != null) {
			commandSB.append(" -Djstorm.home=");
			commandSB.append(stormhome);
		} 
		
		commandSB.append(getLogParameter(totalConf, stormhome, assignment.getTopologyName(), port));

		String classpath = getClassPath(stormjar, stormhome, totalConf);
		String workerClassPath = (String) totalConf
				.get(Config.WORKER_CLASSPATH);
		List<String> otherLibs = (List<String>) stormConf
				.get(GenericOptionsParser.TOPOLOGY_LIB_NAME);
		StringBuilder sb = new StringBuilder();
		if (otherLibs != null) {
			for (String libName : otherLibs) {
				sb.append(StormConfig.stormlib_path(stormroot, libName))
						.append(":");
			}
		}
		workerClassPath = workerClassPath + ":" + sb.toString();

		Map<String, String> policyReplaceMap = new HashMap<String, String>();
		String realClassPath = classpath + ":" + workerClassPath;
		policyReplaceMap.put(SandBoxMaker.CLASS_PATH_KEY, realClassPath);
		commandSB
				.append(sandBoxMaker.sandboxPolicy(workerId, policyReplaceMap));

		// commandSB.append(" -Dlog4j.configuration=storm.log.properties");

		commandSB.append(" -cp ");
		// commandSB.append(workerClassPath + ":");
		commandSB.append(classpath);
		if (!ConfigExtension.isEnableTopologyClassLoader(totalConf))
			commandSB.append(":").append(workerClassPath);

		commandSB.append(" com.alibaba.jstorm.daemon.worker.Worker ");
		commandSB.append(topologyId);

		commandSB.append(" ");
		commandSB.append(supervisorId);

		commandSB.append(" ");
		commandSB.append(port);

		commandSB.append(" ");
		commandSB.append(workerId);

		commandSB.append(" ");
		commandSB.append(workerClassPath + ":" + stormjar);

		String cmd = commandSB.toString();
		cmd = cmd.replace("%ID%", port.toString());
		cmd = cmd.replace("%TOPOLOGYID%", topologyId);
		if (stormhome != null) {
			cmd = cmd.replace("%JSTORM_HOME%", stormhome);
		} else {
			cmd = cmd.replace("%JSTORM_HOME%", "./");
		}
		
		LOG.info("Launching worker with command: " + cmd);
		LOG.info("Environment:" + environment.toString());

		JStormUtils.launch_process(cmd, environment, true);
	}

	private Set<Integer> killUselessWorkers(
			Map<String, StateHeartbeat> localWorkerStats) {
		Map<String, String> removed = new HashMap<String, String>();
		Set<Integer> keepPorts = new HashSet<Integer>();

		for (Entry<String, StateHeartbeat> entry : localWorkerStats.entrySet()) {

			String workerid = entry.getKey();
			StateHeartbeat hbstate = entry.getValue();
			if (workerIdToStartTimeAndPort.containsKey(workerid))
				continue;

			if (hbstate.getState().equals(State.valid)) {
				// hbstate.getHeartbeat() won't be null
				keepPorts.add(hbstate.getHeartbeat().getPort());
			} else {
				if (hbstate.getHeartbeat() != null) {
					removed.put(workerid, hbstate.getHeartbeat()
							.getTopologyId());
				} else {
					removed.put(workerid, null);
				}

				StringBuilder sb = new StringBuilder();
				sb.append("Shutting down and clearing state for id ");
				sb.append(workerid);
				sb.append(";State:");
				sb.append(hbstate);

				LOG.info(sb);
			}
		}

		shutWorker(conf, supervisorId, removed, workerThreadPids, cgroupManager);

		for (String removedWorkerId : removed.keySet()) {
			localWorkerStats.remove(removedWorkerId);
		}
		// new workers need be keep
		for (Entry<String, Pair<Integer, Integer>> entry : workerIdToStartTimeAndPort
				.entrySet()) {
			keepPorts.add(entry.getValue().getSecond());
		}

		return keepPorts;
	}

	private void startNewWorkers(Set<Integer> keepPorts,
			Map<Integer, LocalAssignment> localAssignments) throws Exception {
		/**
		 * Step 4: get reassigned tasks, which is in assignedTasks, but not in
		 * keeperPorts Map<port(type Integer), LocalAssignment>
		 */
		Map<Integer, LocalAssignment> newWorkers = JStormUtils
				.select_keys_pred(keepPorts, localAssignments);

		/**
		 * Step 5: generate new work ids
		 */
		Map<Integer, String> newWorkerIds = new HashMap<Integer, String>();

		for (Entry<Integer, LocalAssignment> entry : newWorkers.entrySet()) {
			Integer port = entry.getKey();
			LocalAssignment assignment = entry.getValue();

			String workerId = UUID.randomUUID().toString();

			newWorkerIds.put(port, workerId);

			// create new worker Id directory
			// LOCALDIR/workers/newworkid/pids
			try {
				StormConfig.worker_pids_root(conf, workerId);
			} catch (IOException e1) {
				LOG.error("Failed to create " + workerId + " localdir", e1);
				throw e1;
			}

			StringBuilder sb = new StringBuilder();
			sb.append("Launching worker with assiangment ");
			sb.append(assignment.toString());
			sb.append(" for the supervisor ");
			sb.append(supervisorId);
			sb.append(" on port ");
			sb.append(port);
			sb.append(" with id ");
			sb.append(workerId);
			LOG.info(sb);

			try {
				String clusterMode = StormConfig.cluster_mode(conf);

				if (clusterMode.equals("distributed")) {
					launchWorker(conf, sharedContext,
							assignment.getTopologyId(), supervisorId, port,
							workerId, assignment);
				} else if (clusterMode.equals("local")) {
					launchWorker(conf, sharedContext,
							assignment.getTopologyId(), supervisorId, port,
							workerId, workerThreadPids);
				}
			} catch (Exception e) {
				String errorMsg = "Failed to launchWorker workerId:" + workerId
						+ ":" + port;
				LOG.error(errorMsg, e);
				throw e;
			}

		}

		/**
		 * FIXME, workerIds should be Set, not Collection, but here simplify the
		 * logic
		 */
		markAllNewWorkers(newWorkerIds);
		// try {
		// waitForWorkersLaunch(conf, workerIds);
		// } catch (IOException e) {
		// LOG.error(e + " waitForWorkersLaunch failed");
		// } catch (InterruptedException e) {
		// LOG.error(e + " waitForWorkersLaunch failed");
		// }
	}
	
	boolean isWorkerDead(String workerId) {
		
		try {
			List<String> pids = getPid(conf, workerId);
			if (pids == null || pids.size() == 0) {
				//local mode doesn't exist pid
				return false;
			}
			// if all pid in pids are dead, then the worker is dead
			for (String pid : pids) {
				boolean isDead = JStormUtils.isProcDead(pid);
				if (isDead == true) {
					LOG.info("Found " + workerId + " is dead " );
				}else {
					return false;
				}
			}
			
			return true;
		} catch (IOException e) {
			LOG.info("Failed to check whether worker is dead through /proc/pid", e);
			return false;
		}
		
		
	}

}
