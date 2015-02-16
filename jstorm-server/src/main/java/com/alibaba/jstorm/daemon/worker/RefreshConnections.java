package com.alibaba.jstorm.daemon.worker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.WorkerSlot;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.alibaba.jstorm.task.TaskInfo;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeat;

/**
 * 
 * Update current worker and other workers' zeroMQ connection.
 * 
 * When worker shutdown/create, need update these connection
 * 
 * @author yannian/Longda
 * 
 */
public class RefreshConnections extends RunnableCallback {
	private static Logger LOG = Logger.getLogger(RefreshConnections.class);

	private WorkerData workerData;

	private AtomicBoolean active;

	@SuppressWarnings("rawtypes")
	private Map conf;

	private StormClusterState zkCluster;

	private String topologyId;

	private Set<Integer> outboundTasks;

	private ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;

	private IContext context;

	private ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;

	private Integer frequence;

	private String supervisorId;
	
	private int taskTimeoutSecs;

	// private ReentrantReadWriteLock endpoint_socket_lock;

	@SuppressWarnings("rawtypes")
	public RefreshConnections(WorkerData workerData, Set<Integer> outbound_tasks) {

		this.workerData = workerData;

		this.active = workerData.getActive();
		this.conf = workerData.getConf();
		this.zkCluster = workerData.getZkCluster();
		this.topologyId = workerData.getTopologyId();
		this.outboundTasks = outbound_tasks;
		this.nodeportSocket = workerData.getNodeportSocket();
		this.context = workerData.getContext();
		this.taskNodeport = workerData.getTaskNodeport();
		this.supervisorId = workerData.getSupervisorId();

		// this.endpoint_socket_lock = endpoint_socket_lock;
		frequence = JStormUtils.parseInt(
				conf.get(Config.TASK_REFRESH_POLL_SECS), 5);
		
		taskTimeoutSecs = JStormUtils.parseInt(conf.get(Config.TASK_HEARTBEAT_FREQUENCY_SECS), 10);
		taskTimeoutSecs = taskTimeoutSecs*3;
	}

	@Override
	public void run() {

		if (active.get() == false) {
			return;
		}

		try {
			//
			// @@@ does lock need?
			// endpoint_socket_lock.writeLock().lock();
			//

			synchronized (this) {
				Assignment assignment = zkCluster.assignment_info(topologyId,
						this);
				if (assignment == null) {
					String errMsg = "Failed to get Assignment of " + topologyId;
					LOG.error(errMsg);
					// throw new RuntimeException(errMsg);
					return;
				}

				Set<ResourceWorkerSlot> workers = assignment.getWorkers();
				if (workers == null) {
					String errMsg = "Failed to get taskToResource of "
							+ topologyId;
					LOG.error(errMsg);
					return;
				}
				workerData.getWorkerToResource().addAll(workers);

				Map<Integer, WorkerSlot> my_assignment = new HashMap<Integer, WorkerSlot>();

				Map<String, String> node = assignment.getNodeHost();

				// only reserve outboundTasks
				Set<WorkerSlot> need_connections = new HashSet<WorkerSlot>();

				Set<Integer> localNodeTasks = new HashSet<Integer>();

				if (workers != null && outboundTasks != null) {
					for (ResourceWorkerSlot worker : workers) {
						if (supervisorId.equals(worker.getNodeId()))
							localNodeTasks.addAll(worker.getTasks());
						for (Integer id : worker.getTasks()) {
							if (outboundTasks.contains(id)) {
								my_assignment.put(id, worker);
								need_connections.add(worker);
							}
						}
					}
				}
				taskNodeport.putAll(my_assignment);
				workerData.setLocalNodeTasks(localNodeTasks);

				// get which connection need to be remove or add
				Set<WorkerSlot> current_connections = nodeportSocket.keySet();
				Set<WorkerSlot> new_connections = new HashSet<WorkerSlot>();
				Set<WorkerSlot> remove_connections = new HashSet<WorkerSlot>();

				for (WorkerSlot node_port : need_connections) {
					if (!current_connections.contains(node_port)) {
						new_connections.add(node_port);
					}
				}

				for (WorkerSlot node_port : current_connections) {
					if (!need_connections.contains(node_port)) {
						remove_connections.add(node_port);
					}
				}

				// create new connection
				for (WorkerSlot nodePort : new_connections) {

					String host = node.get(nodePort.getNodeId());

					int port = nodePort.getPort();

					IConnection conn = context.connect(topologyId, host, port);

					nodeportSocket.put(nodePort, conn);

					LOG.info("Add connection to " + nodePort);
				}

				// close useless connection
				for (WorkerSlot node_port : remove_connections) {
					LOG.info("Remove connection to " + node_port);
					nodeportSocket.remove(node_port).close();
				}
				
				// Update the status of all outbound tasks
				for (Integer taskId : outboundTasks) {
					boolean isActive = false;
					int currentTime = TimeUtils.current_time_secs();
					TaskHeartbeat tHB = zkCluster.task_heartbeat(topologyId, taskId);
					if (tHB != null) {
					    int taskReportTime = tHB.getTimeSecs();
					    if ((currentTime - taskReportTime) < taskTimeoutSecs)
						    isActive = true;
					}
				    workerData.updateOutboundTaskStatus(taskId, isActive);
				}
			}
		} catch (Exception e) {
			LOG.error("Failed to refresh worker Connection", e);
			throw new RuntimeException(e);
		}

		// finally {
		// endpoint_socket_lock.writeLock().unlock();
		// }

	}

	@Override
	public Object getResult() {
		if (active.get()) {
			return frequence;
		}
		return -1;
	}

}
