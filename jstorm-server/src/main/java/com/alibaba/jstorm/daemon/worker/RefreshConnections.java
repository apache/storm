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
import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.utils.JStormUtils;

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

		// this.endpoint_socket_lock = endpoint_socket_lock;
		frequence = JStormUtils.parseInt(
				conf.get(Config.TASK_REFRESH_POLL_SECS), 5);
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

			Assignment assignment = zkCluster.assignment_info(topologyId, this);
			if (assignment == null) {
				String errMsg = "Failed to get Assignment of " + topologyId;
				LOG.error(errMsg);
				// throw new RuntimeException(errMsg);
				return;
			}

			Map<Integer, ResourceAssignment> taskToResource = assignment
					.getTaskToResource();
			if (taskToResource == null) {
				String errMsg = "Failed to get taskToResource of " + topologyId;
				LOG.error(errMsg);
				return;
			}
			workerData.getTaskToResource().putAll(taskToResource);

			Map<Integer, WorkerSlot> my_assignment = new HashMap<Integer, WorkerSlot>();

			Map<String, String> node = assignment.getNodeHost();

			// only reserve outboundTasks
			Set<WorkerSlot> need_connections = new HashSet<WorkerSlot>();

			if (taskToResource != null && outboundTasks != null) {
				for (Entry<Integer, ResourceAssignment> mm : taskToResource
						.entrySet()) {
					int taks_id = mm.getKey();
					ResourceAssignment resource = mm.getValue();
					if (outboundTasks.contains(taks_id)) {
						WorkerSlot workerSlot = new WorkerSlot(
								resource.getSupervisorId(), resource.getPort());
						my_assignment.put(taks_id, workerSlot);
						need_connections.add(workerSlot);
					}
				}
			}
			taskNodeport.putAll(my_assignment);

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

				IConnection conn = context
						.connect(topologyId, host, port, true);

				nodeportSocket.put(nodePort, conn);

				LOG.info("Add connection to " + nodePort);
			}

			// close useless connection
			for (WorkerSlot node_port : remove_connections) {
				LOG.info("Remove connection to " + node_port);
				nodeportSocket.remove(node_port).close();
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
