package com.alibaba.jstorm.daemon.nimbus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.thrift7.TException;

import backtype.storm.Config;
import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.SupervisorWorkers;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologyInitialStatus;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.WorkerSummary;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.DaemonCommon;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.supervisor.SupervisorInfo;
import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.utils.FailedAssignTopologyException;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.alibaba.jstorm.utils.Thrift;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * Thrift callback, all commands handling entrance
 * 
 * @author version 1: lixin, version 2:Longda
 * 
 */
public class ServiceHandler implements Iface, Shutdownable, DaemonCommon {
	private final static Logger LOG = Logger.getLogger(ServiceHandler.class);

	public final static int THREAD_NUM = 64;

	private NimbusData data;

	private Map<Object, Object> conf;

	public ServiceHandler(NimbusData data) {
		this.data = data;
		conf = data.getConf();
	}

	@Override
	public void submitTopology(String name, String uploadedJarLocation,
			String jsonConf, StormTopology topology)
			throws AlreadyAliveException, InvalidTopologyException,
			TopologyAssignException, TException {
		SubmitOptions options = new SubmitOptions(TopologyInitialStatus.ACTIVE);

		submitTopologyWithOpts(name, uploadedJarLocation, jsonConf, topology,
				options);
	}

	/**
	 * Submit one Topology
	 * 
	 * @param topologyname
	 *            String: topology name
	 * @param uploadedJarLocation
	 *            String: already uploaded jar path
	 * @param jsonConf
	 *            String: jsonConf serialize all toplogy configuration to Json
	 * @param topology
	 *            StormTopology: topology Object
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void submitTopologyWithOpts(String topologyname,
			String uploadedJarLocation, String jsonConf,
			StormTopology topology, SubmitOptions options)
			throws AlreadyAliveException, InvalidTopologyException,
			TopologyAssignException, TException {
		LOG.info("Receive " + topologyname + ", uploadedJarLocation:"
				+ uploadedJarLocation);
		// @@@ Move validate topologyname in client code
		try {
			checkTopologyActive(data, topologyname, false);
		} catch (AlreadyAliveException e) {
			LOG.info(topologyname + " is already exist ");
			throw e;
		} catch (Exception e) {
			LOG.info("Failed to check whether topology is alive or not", e);
			throw new TException(e);
		}

		int counter = data.getSubmittedCount().incrementAndGet();
		String topologyId = topologyname + "-" + counter + "-"
				+ TimeUtils.current_time_secs();

		Map<Object, Object> serializedConf = (Map<Object, Object>) JStormUtils
				.from_json(jsonConf);
		if (serializedConf == null) {
			LOG.warn("Failed to serialized Configuration");
			throw new InvalidTopologyException(
					"Failed to serilaze topology configuration");
		}

		serializedConf.put(Config.TOPOLOGY_ID, topologyId);
		serializedConf.put(Config.TOPOLOGY_NAME, topologyname);

		

		try {
			Map<Object, Object> stormConf;
			
			stormConf = NimbusUtils.normalizeConf(conf, serializedConf,
					topology);
			
			Map<Object, Object> totalStormConf = new HashMap<Object, Object>(conf);
			totalStormConf.putAll(stormConf);

			StormTopology normalizedTopology = NimbusUtils.normalizeTopology(
					stormConf, topology);

			// this validates the structure of the topology
			Common.validate_basic(normalizedTopology, totalStormConf, topologyId);
			// don't need generate real topology, so skip Common.system_topology
			// Common.system_topology(totalStormConf, topology);

			StormClusterState stormClusterState = data.getStormClusterState();

			// create /local-dir/nimbus/topologyId/xxxx files
			setupStormCode(conf, topologyId, uploadedJarLocation, stormConf,
					normalizedTopology);

			// generate TaskInfo for every bolt or spout in ZK
			// /ZK/tasks/topoologyId/xxx
			setupZkTaskInfo(conf, topologyId, stormClusterState);

			// make assignments for a topology
			TopologyAssignEvent assignEvent = new TopologyAssignEvent();
			assignEvent.setTopologyId(topologyId);
			assignEvent.setScratch(false);
			assignEvent.setTopologyName(topologyname);
			assignEvent.setOldStatus(Thrift
					.topologyInitialStatusToStormStatus(options
							.get_initial_status()));

			TopologyAssign.push(assignEvent);
			LOG.info("Submit for " + topologyname + " with conf "
					+ serializedConf);

			boolean isSuccess = assignEvent.waitFinish();
			if (isSuccess == true) {
				LOG.info("Finish submit for " + topologyname);
			} else {
				throw new FailedAssignTopologyException(
						assignEvent.getErrorMsg());
			}

		} catch (FailedAssignTopologyException e) {
			StringBuilder sb = new StringBuilder();
			sb.append("Fail to sumbit topology, Root cause:");
			if (e.getMessage() == null) {
				sb.append("submit timeout");
			} else {
				sb.append(e.getMessage());
			}

			sb.append("\n\n");
			sb.append("topologyId:" + topologyId);
			sb.append(", uploadedJarLocation:" + uploadedJarLocation + "\n");
			LOG.error(sb.toString(), e);
			throw new TopologyAssignException(sb.toString());
		}catch (InvalidParameterException e) {
			StringBuilder sb = new StringBuilder();
			sb.append("Fail to sumbit topology ");
			sb.append(e.getMessage());
			sb.append(", cause:" + e.getCause());
			sb.append("\n\n");
			sb.append("topologyId:" + topologyId);
			sb.append(", uploadedJarLocation:" + uploadedJarLocation + "\n");
			LOG.error(sb.toString(), e);
			throw new InvalidParameterException(sb.toString());
		} catch (Throwable e) {
			StringBuilder sb = new StringBuilder();
			sb.append("Fail to sumbit topology ");
			sb.append(e.getMessage());
			sb.append(", cause:" + e.getCause());
			sb.append("\n\n");
			sb.append("topologyId:" + topologyId);
			sb.append(", uploadedJarLocation:" + uploadedJarLocation + "\n");
			LOG.error(sb.toString(), e);
			throw new TopologyAssignException(sb.toString());
		}

	}

	/**
	 * kill topology
	 * 
	 * @param topologyname
	 *            String topology name
	 */
	@Override
	public void killTopology(String name) throws NotAliveException, TException {
		killTopologyWithOpts(name, new KillOptions());

	}

	@Override
	public void killTopologyWithOpts(String topologyName, KillOptions options)
			throws NotAliveException, TException {
		try {

			checkTopologyActive(data, topologyName, true);
			Integer wait_amt = null;
			if (options.is_set_wait_secs()) {
				wait_amt = options.get_wait_secs();
			}
			NimbusUtils.transitionName(data, topologyName, true,
					StatusType.kill, wait_amt);
		} catch (NotAliveException e) {
			String errMsg = "KillTopology Error, no this topology "
					+ topologyName;
			LOG.error(errMsg, e);
			throw new NotAliveException(errMsg);
		} catch (Exception e) {
			String errMsg = "Failed to kill topology " + topologyName;
			LOG.error(errMsg, e);
			throw new TException(errMsg);
		}

	}

	/**
	 * set topology status as active
	 * 
	 * @param topologyname
	 * 
	 */
	@Override
	public void activate(String topologyName) throws NotAliveException,
			TException {
		try {
			NimbusUtils.transitionName(data, topologyName, true,
					StatusType.activate);
		} catch (NotAliveException e) {
			String errMsg = "Activate Error, no this topology " + topologyName;
			LOG.error(errMsg, e);
			throw new NotAliveException(errMsg);
		} catch (Exception e) {
			String errMsg = "Failed to active topology " + topologyName;
			LOG.error(errMsg, e);
			throw new TException(errMsg);
		}

	}

	/**
	 * set topology stauts as deactive
	 * 
	 * @param topologyname
	 * 
	 */
	@Override
	public void deactivate(String topologyName) throws NotAliveException,
			TException {

		try {
			NimbusUtils.transitionName(data, topologyName, true,
					StatusType.inactivate);
		} catch (NotAliveException e) {
			String errMsg = "Deactivate Error, no this topology "
					+ topologyName;
			LOG.error(errMsg, e);
			throw new NotAliveException(errMsg);
		} catch (Exception e) {
			String errMsg = "Failed to deactivate topology " + topologyName;
			LOG.error(errMsg, e);
			throw new TException(errMsg);
		}

	}

	/**
	 * rebalance one topology
	 * 
	 * @@@ rebalance options hasn't implements
	 * 
	 *     It is used to let workers wait several seconds to finish jobs
	 * 
	 * @param topologyname
	 *            String
	 * @param options
	 *            RebalanceOptions
	 */
	@Override
	public void rebalance(String topologyName, RebalanceOptions options)
			throws NotAliveException, TException, InvalidTopologyException {

		try {

			checkTopologyActive(data, topologyName, true);
			Integer wait_amt = null;
			if (options != null && options.is_set_wait_secs()) {
				wait_amt = options.get_wait_secs();
			}

			NimbusUtils.transitionName(data, topologyName, true,
					StatusType.rebalance, wait_amt);
		} catch (NotAliveException e) {
			String errMsg = "Rebalance Error, no this topology " + topologyName;
			LOG.error(errMsg, e);
			throw new NotAliveException(errMsg);
		} catch (Exception e) {
			String errMsg = "Failed to rebalance topology " + topologyName;
			LOG.error(errMsg, e);
			throw new TException(errMsg);
		}

	}

	/**
	 * prepare to uploading topology jar, return the file location
	 * 
	 * @throws
	 */
	@Override
	public String beginFileUpload() throws TException {
		String fileLoc = null;
		try {
			fileLoc = StormConfig.masterInbox(conf) + "/stormjar-"
					+ UUID.randomUUID() + ".jar";

			data.getUploaders().put(fileLoc,
					Channels.newChannel(new FileOutputStream(fileLoc)));
			LOG.info("Uploading file from client to " + fileLoc);
		} catch (FileNotFoundException e) {
			LOG.error(" file not found " + fileLoc);
			throw new TException(e);
		} catch (IOException e) {
			LOG.error(" IOException  " + fileLoc, e);
			throw new TException(e);
		}
		return fileLoc;
	}

	/**
	 * uploading topology jar data
	 */
	@Override
	public void uploadChunk(String location, ByteBuffer chunk)
			throws TException {
		TimeCacheMap<Object, Object> uploaders = data.getUploaders();
		Object obj = uploaders.get(location);
		if (obj == null) {
			throw new TException(
					"File for that location does not exist (or timed out) "
							+ location);
		}
		try {
			if (obj instanceof WritableByteChannel) {
				WritableByteChannel channel = (WritableByteChannel) obj;
				channel.write(chunk);
				uploaders.put(location, channel);
			} else {
				throw new TException("Object isn't WritableByteChannel for "
						+ location);
			}
		} catch (IOException e) {
			String errMsg = " WritableByteChannel write filed when uploadChunk "
					+ location;
			LOG.error(errMsg);
			throw new TException(e);
		}

	}

	@Override
	public void finishFileUpload(String location) throws TException {

		TimeCacheMap<Object, Object> uploaders = data.getUploaders();
		Object obj = uploaders.get(location);
		if (obj == null) {
			throw new TException(
					"File for that location does not exist (or timed out)");
		}
		try {
			if (obj instanceof WritableByteChannel) {
				WritableByteChannel channel = (WritableByteChannel) obj;
				channel.close();
				uploaders.remove(location);
				LOG.info("Finished uploading file from client: " + location);
			} else {
				throw new TException("Object isn't WritableByteChannel for "
						+ location);
			}
		} catch (IOException e) {
			LOG.error(" WritableByteChannel close failed when finishFileUpload "
					+ location);
		}

	}

	@Override
	public String beginFileDownload(String file) throws TException {
		BufferFileInputStream is = null;
		String id = null;
		try {
			is = new BufferFileInputStream(file);
			id = UUID.randomUUID().toString();
			data.getDownloaders().put(id, is);
		} catch (FileNotFoundException e) {
			LOG.error(e + "file:" + file + " not found");
			throw new TException(e);
		}

		return id;
	}

	@Override
	public ByteBuffer downloadChunk(String id) throws TException {
		TimeCacheMap<Object, Object> downloaders = data.getDownloaders();
		Object obj = downloaders.get(id);
		if (obj == null) {
			throw new TException("Could not find input stream for that id");
		}
		
		try {
			if (obj instanceof BufferFileInputStream) {
				BufferFileInputStream is = (BufferFileInputStream) obj;
				byte[] ret = is.read();
				if (ret != null) {
					downloaders.put(id, (BufferFileInputStream) is);
					return ByteBuffer.wrap(ret);
				}
			} else {
				throw new TException("Object isn't BufferFileInputStream for "
						+ id);
			}
		} catch (IOException e) {
			LOG.error("BufferFileInputStream read failed when downloadChunk ", e);
			throw new TException(e);
		}
        byte[] empty = {};
		return ByteBuffer.wrap(empty);
	}

	/**
	 * get cluster's summary, it will contain SupervisorSummary and
	 * TopologySummary
	 * 
	 * @return ClusterSummary
	 */
	@Override
	public ClusterSummary getClusterInfo() throws TException {

		try {

			StormClusterState stormClusterState = data.getStormClusterState();

			Map<String, Assignment> assignments = new HashMap<String, Assignment>();

			// get nimbus running time
			int uptime = data.uptime();

			// get TopologySummary
			List<TopologySummary> topologySummaries = new ArrayList<TopologySummary>();

			// get all active topology's StormBase
			Map<String, StormBase> bases = Cluster
					.topology_bases(stormClusterState);
			for (Entry<String, StormBase> entry : bases.entrySet()) {

				String topologyId = entry.getKey();
				StormBase base = entry.getValue();

				Assignment assignment = stormClusterState.assignment_info(
						topologyId, null);
				if (assignment == null) {
					LOG.error("Failed to get assignment of " + topologyId);
					continue;
				}
				assignments.put(topologyId, assignment);
				String group = "default";
				if (data.isGroupMode())
					group = base.getGroup();
				if (group == null)
					group = "default";

				TopologySummary topology = NimbusUtils.mkTopologySummary(
						assignment, topologyId, base.getStormName(),
						base.getStatusString(),
						TimeUtils.time_delta(base.getLanchTimeSecs()), group);

				topologySummaries.add(topology);

			}

			// all supervisors
			Map<String, SupervisorInfo> supervisorInfos = Cluster
					.allSupervisorInfo(stormClusterState, null);

			// generate SupervisorSummaries
			List<SupervisorSummary> supervisorSummaries = NimbusUtils
					.mkSupervisorSummaries(supervisorInfos, assignments);

			return new ClusterSummary(supervisorSummaries, uptime,
					topologySummaries, data.getGroupToTopology(),
					data.getGroupToResource(), data.getGroupToUsedResource(),
					data.isGroupMode());

		} catch (TException e) {
			LOG.info("Failed to get ClusterSummary ", e);
			throw e;
		} catch (Exception e) {
			LOG.info("Failed to get ClusterSummary ", e);
			throw new TException(e);
		}
	}

	@Override
	public SupervisorWorkers getSupervisorWorkers(String host)
			throws NotAliveException, TException {
		try {
			StormClusterState stormClusterState = data.getStormClusterState();

			String supervisorId = null;
			SupervisorInfo supervisorInfo = null;
			
			String ip = NetWorkUtils.host2Ip(host);
			String hostName = NetWorkUtils.ip2Host(host);

			// all supervisors
			Map<String, SupervisorInfo> supervisorInfos = Cluster
					.allSupervisorInfo(stormClusterState, null);

			for (Entry<String, SupervisorInfo> entry : supervisorInfos
					.entrySet()) {

				SupervisorInfo info = entry.getValue();
				if (info.getHostName().equals(hostName) ||
					 info.getHostName().equals(ip)) {
					supervisorId = entry.getKey();
					supervisorInfo = info;
					break;
				}
			}

			if (supervisorId == null) {
				throw new TException("No supervisor of " + host);
			}

			Map<String, Assignment> assignments = new HashMap<String, Assignment>();

			// get all active topology's StormBase
			Map<String, StormBase> bases = Cluster
					.topology_bases(stormClusterState);
			for (Entry<String, StormBase> entry : bases.entrySet()) {

				String topologyId = entry.getKey();
				StormBase base = entry.getValue();

				Assignment assignment = stormClusterState.assignment_info(
						topologyId, null);
				if (assignment == null) {
					LOG.error("Failed to get assignment of " + topologyId);
					continue;
				}
				assignments.put(topologyId, assignment);

			}

			Map<Integer, WorkerSummary> portWorkerSummarys = new TreeMap<Integer, WorkerSummary>();
			for (Entry<String, Assignment> entry : assignments.entrySet()) {
				String topologyId = entry.getKey();
				Assignment assignment = entry.getValue();

				Map<Integer, String> taskToComponent = Cluster
						.topology_task_info(stormClusterState, topologyId);

				Map<Integer, ResourceAssignment> taskToResource = assignment
						.getTaskToResource();

				for (Entry<Integer, ResourceAssignment> resourceEntry : taskToResource
						.entrySet()) {
					Integer taskId = resourceEntry.getKey();
					ResourceAssignment resourceAssignment = resourceEntry
							.getValue();

					if (supervisorId.equals(resourceAssignment
							.getSupervisorId()) == false) {
						continue;
					}

					supervisorInfo.allocResource(resourceAssignment);

					Integer port = resourceAssignment.getPort();
					WorkerSummary workerSummary = portWorkerSummarys.get(port);
					if (workerSummary == null) {
						workerSummary = new WorkerSummary();
						workerSummary.set_port(port);
						workerSummary.set_topology(topologyId);
						workerSummary.set_tasks(new ArrayList<TaskSummary>());

						portWorkerSummarys.put(port, workerSummary);
					}

					String componentName = taskToComponent.get(taskId);
					int uptime = TimeUtils.time_delta(assignment
							.getTaskStartTimeSecs().get(taskId));
					List<TaskSummary> tasks = workerSummary.get_tasks();

					TaskSummary taskSummary = NimbusUtils.mkSimpleTaskSummary(
							resourceAssignment, taskId, componentName, host,
							uptime);

					tasks.add(taskSummary);
				}
			}

			List<WorkerSummary> wokersList = new ArrayList<WorkerSummary>();
			wokersList.addAll(portWorkerSummarys.values());

			SupervisorSummary supervisorSummary = NimbusUtils
					.mkSupervisorSummary(supervisorInfo, supervisorId);
			return new SupervisorWorkers(supervisorSummary, wokersList);

		} catch (TException e) {
			LOG.info("Failed to get ClusterSummary ", e);
			throw e;
		} catch (Exception e) {
			LOG.info("Failed to get ClusterSummary ", e);
			throw new TException(e);
		}
	}

	/**
	 * Get TopologyInfo, it contain all data of the topology running status
	 * 
	 * @return TopologyInfo
	 */
	@Override
	public TopologyInfo getTopologyInfo(String topologyId)
			throws NotAliveException, TException {

		TopologyInfo topologyInfo = new TopologyInfo();

		StormClusterState stormClusterState = data.getStormClusterState();

		try {

			// get topology's StormBase
			StormBase base = stormClusterState.storm_base(topologyId, null);
			if (base == null) {
				throw new NotAliveException("No topology of " + topologyId);
			}
			topologyInfo.set_id(topologyId);
			topologyInfo.set_name(base.getStormName());
			topologyInfo.set_uptime_secs(TimeUtils.time_delta(base
					.getLanchTimeSecs()));
			topologyInfo.set_status(base.getStatusString());

			// get topology's Assignment
			Assignment assignment = stormClusterState.assignment_info(
					topologyId, null);
			if (assignment == null) {
				throw new TException("Failed to get StormBase from ZK of "
						+ topologyId);
			}

			// get topology's map<taskId, componentId>
			Map<Integer, String> taskInfo = Cluster.topology_task_info(
					stormClusterState, topologyId);

			List<TaskSummary> tasks = NimbusUtils.mkTaskSummary(
					stormClusterState, assignment, taskInfo, topologyId);
			topologyInfo.set_tasks(tasks);

			return topologyInfo;
		} catch (TException e) {
			LOG.info("Failed to get topologyInfo " + topologyId, e);
			throw e;
		} catch (Exception e) {
			LOG.info("Failed to get topologyInfo " + topologyId, e);
			throw new TException("Failed to get topologyInfo" + topologyId);
		}

	}

	/**
	 * get topology configuration
	 * 
	 * @param id
	 *            String: topology id
	 * @return String
	 */
	@Override
	public String getTopologyConf(String id) throws NotAliveException,
			TException {
		String rtn;
		try {
			Map<Object, Object> topologyConf = StormConfig
					.read_nimbus_topology_conf(conf, id);
			rtn = JStormUtils.to_json(topologyConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.info("Failed to get configuration of " + id, e);
			throw new TException(e);
		}
		return rtn;
	}

	/**
	 * get StormTopology throw deserialize local files
	 * 
	 * @param id
	 *            String: topology id
	 * @return StormTopology
	 */
	@Override
	public StormTopology getTopology(String id) throws NotAliveException,
			TException {
		StormTopology topology = null;
		try {
			StormTopology stormtopology = StormConfig
					.read_nimbus_topology_code(conf, id);
			if (stormtopology == null) {
				throw new TException("topology:" + id + "is null");
			}

			Map<Object, Object> topologyConf = (Map<Object, Object>) StormConfig
					.read_nimbus_topology_conf(conf, id);

			topology = Common.system_topology(topologyConf, stormtopology);
		} catch (Exception e) {
			LOG.error("Failed to get topology " + id + ",", e);
			throw new TException("Failed to get system_topology");
		}
		return topology;
	}

	/**
	 * Shutdown the nimbus
	 */
	@Override
	public void shutdown() {
		LOG.info("Begin to shut down master");
		// Timer.cancelTimer(nimbus.getTimer());

		LOG.info("Successfully shut down master");

	}

	@Override
	public boolean waiting() {
		// @@@ TODO
		return false;
	}

	/**
	 * check whether the topology is bActive?
	 * 
	 * @param nimbus
	 * @param topologyName
	 * @param bActive
	 * @throws Exception
	 */
	public void checkTopologyActive(NimbusData nimbus, String topologyName,
			boolean bActive) throws Exception {
		if (isTopologyActive(nimbus.getStormClusterState(), topologyName) != bActive) {
			if (bActive) {
				throw new NotAliveException(topologyName + " is not alive");
			} else {
				throw new AlreadyAliveException(topologyName
						+ " is already active");
			}
		}
	}

	/**
	 * whether the topology is active by topology name
	 * 
	 * @param stormClusterState
	 *            see Cluster_clj
	 * @param topologyName
	 * @return boolean if the storm is active, return true, otherwise return
	 *         false;
	 * @throws Exception
	 */
	public boolean isTopologyActive(StormClusterState stormClusterState,
			String topologyName) throws Exception {
		boolean rtn = false;
		if (Cluster.get_topology_id(stormClusterState, topologyName) != null) {
			rtn = true;
		}
		return rtn;
	}

	/**
	 * create local topology files /local-dir/nimbus/topologyId/stormjar.jar
	 * /local-dir/nimbus/topologyId/stormcode.ser
	 * /local-dir/nimbus/topologyId/stormconf.ser
	 * 
	 * @param conf
	 * @param topologyId
	 * @param tmpJarLocation
	 * @param stormConf
	 * @param topology
	 * @throws IOException
	 */
	private void setupStormCode(Map<Object, Object> conf, String topologyId,
			String tmpJarLocation, Map<Object, Object> stormConf,
			StormTopology topology) throws IOException {
		// local-dir/nimbus/stormdist/topologyId
		String stormroot = StormConfig.masterStormdistRoot(conf, topologyId);

		FileUtils.forceMkdir(new File(stormroot));
		FileUtils.cleanDirectory(new File(stormroot));

		// copy jar to /local-dir/nimbus/topologyId/stormjar.jar
		setupJar(conf, tmpJarLocation, stormroot);

		// serialize to file /local-dir/nimbus/topologyId/stormcode.ser
		FileUtils.writeByteArrayToFile(
				new File(StormConfig.stormcode_path(stormroot)),
				Utils.serialize(topology));

		// serialize to file /local-dir/nimbus/topologyId/stormconf.ser
		FileUtils.writeByteArrayToFile(
				new File(StormConfig.sotrmconf_path(stormroot)),
				Utils.serialize(stormConf));
	}

	/**
	 * Copy jar to /local-dir/nimbus/topologyId/stormjar.jar
	 * 
	 * @param conf
	 * @param tmpJarLocation
	 * @param stormroot
	 * @throws IOException
	 */
	private void setupJar(Map<Object, Object> conf, String tmpJarLocation,
			String stormroot) throws IOException {
		if (!StormConfig.local_mode(conf)) {
			File srcFile = new File(tmpJarLocation);
			if (!srcFile.exists()) {
				throw new IllegalArgumentException(tmpJarLocation
						+ " to copy to " + stormroot + " does not exist!");
			}
			String path = StormConfig.stormjar_path(stormroot);
			File destFile = new File(path);
			FileUtils.copyFile(srcFile, destFile);
		}
	}

	/**
	 * generate TaskInfo for every bolt or spout in ZK /ZK/tasks/topoologyId/xxx
	 * 
	 * @param conf
	 * @param topologyId
	 * @param stormClusterState
	 * @throws Exception
	 */
	public void setupZkTaskInfo(Map<Object, Object> conf, String topologyId,
			StormClusterState stormClusterState) throws Exception {

		// mkdir /ZK/taskbeats/topoologyId
		stormClusterState.setup_heartbeats(topologyId);

		Map<Integer, String> taskToComponetId = mkTaskComponentAssignments(
				conf, topologyId);
		if (taskToComponetId == null) {
			throw new InvalidTopologyException("Failed to generate TaskIDs map");
		}

		for (Entry<Integer, String> entry : taskToComponetId.entrySet()) {
			// key is taskid, value is taskinfo

			TaskInfo taskinfo = new TaskInfo(entry.getValue());

			stormClusterState.set_task(topologyId, entry.getKey(), taskinfo);
		}
	}

	/**
	 * generate a taskid(Integer) for every task
	 * 
	 * @param conf
	 * @param topologyid
	 * @return Map<Integer, String>: from taskid to componentid
	 * @throws IOException
	 * @throws InvalidTopologyException
	 */
	public Map<Integer, String> mkTaskComponentAssignments(
			Map<Object, Object> conf, String topologyid) throws IOException,
			InvalidTopologyException {

		// @@@ here exist a little problem,
		// we can directly pass stormConf from Submit method
		Map<Object, Object> stormConf = StormConfig.read_nimbus_topology_conf(
				conf, topologyid);

		StormTopology stopology = StormConfig.read_nimbus_topology_code(conf,
				topologyid);

		// use TreeMap to make task as sequence
		Map<Integer, String> rtn = new TreeMap<Integer, String>();

		StormTopology topology = Common.system_topology(stormConf, stopology);

		Integer count = 0;
		count = mkTaskMaker(stormConf, topology.get_bolts(), rtn, count);
		count = mkTaskMaker(stormConf, topology.get_spouts(), rtn, count);
		count = mkTaskMaker(stormConf, topology.get_state_spouts(), rtn, count);

		return rtn;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Integer mkTaskMaker(Map<Object, Object> stormConf,
			Map<String, ?> cidSpec, Map<Integer, String> rtn, Integer cnt) {
		if (cidSpec == null) {
			LOG.warn("Component map is empty");
			return cnt;
		}

		Set<?> entrySet = cidSpec.entrySet();
		for (Iterator<?> it = entrySet.iterator(); it.hasNext();) {
			Entry entry = (Entry) it.next();
			Object obj = entry.getValue();

			ComponentCommon common = null;
			if (obj instanceof Bolt) {
				common = ((Bolt) obj).get_common();

			} else if (obj instanceof SpoutSpec) {
				common = ((SpoutSpec) obj).get_common();

			} else if (obj instanceof StateSpoutSpec) {
				common = ((StateSpoutSpec) obj).get_common();

			}

			if (common == null) {
				throw new RuntimeException("No ComponentCommon of " + entry.getKey());
			}
			
			int declared = Thrift.parallelismHint(common);
			Integer parallelism = declared;
			// Map tmp = (Map) Utils_clj.from_json(common.get_json_conf());

			Map newStormConf = new HashMap(stormConf);
			// newStormConf.putAll(tmp);
			Integer maxParallelism = JStormUtils.parseInt(newStormConf
					.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM));
			if (maxParallelism != null) {
				parallelism = Math.min(maxParallelism, declared);
			}

			for (int i = 0; i < parallelism; i++) {
				cnt++;
				rtn.put(cnt, (String) entry.getKey());
			}
		}
		return cnt;
	}

	@Override
	public String getNimbusConf() throws TException {
		return null;
	}

	@Override
	public StormTopology getUserTopology(String id) throws NotAliveException,
			TException {
		return null;
	}

}
