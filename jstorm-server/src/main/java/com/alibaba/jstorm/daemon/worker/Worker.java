package com.alibaba.jstorm.daemon.worker;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.messaging.IContext;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskShutdownDameon;

/**
 * worker entrance
 * 
 * @author yannian/Longda
 * 
 */
public class Worker {

	private static Logger LOG = Logger.getLogger(Worker.class);

	/**
	 * Why need workerData, it is for thread comeptition
	 */
	private WorkerData workerData;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Worker(Map conf, IContext context, String topology_id,
			String supervisor_id, int port, String worker_id, String jar_path)
			throws Exception {

		workerData = new WorkerData(conf, context, topology_id, supervisor_id,
				port, worker_id, jar_path);

	}

	/**
	 * get current task's output task list
	 * 
	 * @param tasks_component
	 * @param mk_topology_context
	 * @param task_ids
	 * @throws Exception
	 */
	public Set<Integer> worker_output_tasks() {

		ContextMaker context_maker = workerData.getContextMaker();
		Set<Integer> task_ids = workerData.getTaskids();
		StormTopology topology = workerData.getSysTopology();

		Set<Integer> rtn = new HashSet<Integer>();

		for (Integer taskid : task_ids) {
			TopologyContext context = context_maker.makeTopologyContext(
					topology, taskid, null);

			// <StreamId, <ComponentId, Grouping>>
			Map<String, Map<String, Grouping>> targets = context
					.getThisTargets();
			for (Map<String, Grouping> e : targets.values()) {
				for (String componentId : e.keySet()) {
					List<Integer> tasks = context
							.getComponentTasks(componentId);
					rtn.addAll(tasks);
				}
			}
		}

		return rtn;
	}

	private RefreshConnections makeRefreshConnections() {

		// get output streams of every task
		Set<Integer> outboundTasks = worker_output_tasks();

		RefreshConnections refresh_connections = new RefreshConnections(
				workerData, outboundTasks);

		return refresh_connections;
	}

	private List<TaskShutdownDameon> createTasks() throws Exception {
		List<TaskShutdownDameon> shutdowntasks = new ArrayList<TaskShutdownDameon>();

		Set<Integer> taskids = workerData.getTaskids();

		for (int taskid : taskids) {

			TaskShutdownDameon t = Task.mk_task(workerData, taskid);

			shutdowntasks.add(t);
		}

		return shutdowntasks;
	}

	public WorkerShutdown execute() throws Exception {

		// shutdown task callbacks
		List<TaskShutdownDameon> shutdowntasks = createTasks();
		workerData.setShutdownTasks(shutdowntasks);

		// create virtual port object
		// when worker receives tupls, dispatch targetTask according to task_id
		// conf, supervisorId, topologyId, port, context, taskids
		WorkerVirtualPort virtual_port = new WorkerVirtualPort(workerData);
		Shutdownable virtual_port_shutdown = virtual_port.launch();

		// refresh connection
		RefreshConnections refreshConn = makeRefreshConnections();
		AsyncLoopThread refreshconn = new AsyncLoopThread(refreshConn, false,
				Thread.MIN_PRIORITY, true);

		// refresh ZK active status
		RefreshActive refreshZkActive = new RefreshActive(workerData);
		AsyncLoopThread refreshzk = new AsyncLoopThread(refreshZkActive, false,
				Thread.MIN_PRIORITY, true);

		// refresh hearbeat to Local dir
		RunnableCallback heartbeat_fn = new WorkerHeartbeatRunable(workerData);
		AsyncLoopThread hb = new AsyncLoopThread(heartbeat_fn, false, null,
				Thread.NORM_PRIORITY, true);

		// transferQueue, nodeportSocket, taskNodeport
		DrainerRunable drainer = new DrainerRunable(workerData);
		AsyncLoopThread dr = new AsyncLoopThread(drainer, false,
				Thread.MAX_PRIORITY, true);

		AsyncLoopThread[] threads = { refreshconn, refreshzk, hb, dr };

		return new WorkerShutdown(workerData, shutdowntasks,
				virtual_port_shutdown, threads);

	}

	/**
	 * create worker instance and run it
	 * 
	 * @param conf
	 * @param mq_context
	 * @param topology_id
	 * @param supervisor_id
	 * @param port
	 * @param worker_id
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public static WorkerShutdown mk_worker(Map conf, IContext context,
			String topology_id, String supervisor_id, int port,
			String worker_id, String jar_path) throws Exception {

		StringBuilder sb = new StringBuilder();
		sb.append("topologyId:" + topology_id + ", ");
		sb.append("port:" + port + ", ");
		sb.append("workerId:" + worker_id + ", ");
		sb.append("jarPath:" + jar_path + "\n");

		LOG.info("Begin to run worker:" + sb.toString());

		Worker w = new Worker(conf, context, topology_id, supervisor_id, port,
				worker_id, jar_path);

		return w.execute();
	}

	public static void redirectOutput(String port) throws Exception {

		if (System.getenv("REDIRECT") == null
				|| !System.getenv("REDIRECT").equals("true"))
			return;

		String OUT_TARGET_FILE = "/dev/null";

		System.out.println("Redirect output to " + OUT_TARGET_FILE);

		FileOutputStream workerOut = new FileOutputStream(new File(
				OUT_TARGET_FILE));

		PrintStream ps = new PrintStream(new BufferedOutputStream(workerOut),
				true);
		System.setOut(ps);

		LOG.info("Successfully redirect System.out to " + OUT_TARGET_FILE);

	}

	/**
	 * worker entrance
	 * 
	 * @param args
	 */
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) {
		if (args.length < 5) {
			StringBuilder sb = new StringBuilder();
			sb.append("The length of args is less than 5 ");
			for (String arg : args) {
				sb.append(arg + " ");
			}
			LOG.error(sb.toString());
			System.exit(-1);
		}

		String topology_id = args[0];
		String supervisor_id = args[1];
		String port_str = args[2];
		String worker_id = args[3];
		String jar_path = args[4];

		Map conf = Utils.readStormConfig();
		StormConfig.validate_distributed_mode(conf);

		StringBuilder sb = new StringBuilder();
		sb.append("topologyId:" + topology_id + ", ");
		sb.append("port:" + port_str + ", ");
		sb.append("workerId:" + worker_id + ", ");
		sb.append("jar_path:" + jar_path + "\n");

		try {
			redirectOutput(port_str);

			WorkerShutdown sd = mk_worker(conf, null, topology_id,
					supervisor_id, Integer.parseInt(port_str), worker_id,
					jar_path);
			sd.join();

			LOG.info("Successfully shutdown worker " + sb.toString());
		} catch (Throwable e) {
			String errMsg = "Failed to create worker, " + sb.toString();
			LOG.error(errMsg, e);
		}
	}

}
