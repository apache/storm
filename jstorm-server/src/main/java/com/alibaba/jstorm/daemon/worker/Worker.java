package com.alibaba.jstorm.daemon.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.hearbeat.SyncContainerHb;
import com.alibaba.jstorm.daemon.worker.hearbeat.WorkerHeartbeatRunable;
import com.alibaba.jstorm.daemon.worker.metrics.MetricReporter;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeatRunable;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

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
		
		workerData.initOutboundTaskStatus(outboundTasks);

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

	private AsyncLoopThread startDispatchThread() {
		Map stormConf = workerData.getStormConf();

		int queue_size = Utils.getInt(
				stormConf.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE), 1024);
		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) stormConf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		DisruptorQueue recvQueue = DisruptorQueue.mkInstance("Dispatch", ProducerType.MULTI,
				queue_size, waitStrategy);
		// stop  consumerStarted
		//recvQueue.consumerStarted();

		IContext context = workerData.getContext();
		String topologyId = workerData.getTopologyId();

		IConnection recvConnection = context.bind(topologyId,
				workerData.getPort());
		recvConnection.registerQueue(recvQueue);

		RunnableCallback recvDispather = new VirtualPortDispatch(workerData,
				recvConnection, recvQueue);

		AsyncLoopThread vthread = new AsyncLoopThread(recvDispather, false,
				Thread.MAX_PRIORITY, false);

		return vthread;
	}

	public WorkerShutdown execute() throws Exception {
		List<AsyncLoopThread> threads = new ArrayList<AsyncLoopThread>();
		
		AsyncLoopThread dispatcher = startDispatchThread();
		threads.add(dispatcher);

		// create client before create task
		// so create client connection before create task
		// refresh connection
		RefreshConnections refreshConn = makeRefreshConnections();
		AsyncLoopThread refreshconn = new AsyncLoopThread(refreshConn, false,
				Thread.MIN_PRIORITY, true);
		threads.add(refreshconn);

		// refresh ZK active status
		RefreshActive refreshZkActive = new RefreshActive(workerData);
		AsyncLoopThread refreshzk = new AsyncLoopThread(refreshZkActive, false,
				Thread.MIN_PRIORITY, true);
		threads.add(refreshzk);

		BatchTupleRunable batchRunable = new BatchTupleRunable(workerData);
		AsyncLoopThread batch = new AsyncLoopThread(batchRunable, false,
				Thread.MAX_PRIORITY, true);
		threads.add(batch);

		// transferQueue, nodeportSocket, taskNodeport
		DrainerRunable drainer = new DrainerRunable(workerData);
		AsyncLoopThread dr = new AsyncLoopThread(drainer, false,
				Thread.MAX_PRIORITY, true);
		threads.add(dr);

		// Sync heartbeat to Apsara Container
		AsyncLoopThread syncContainerHbThread = SyncContainerHb
				.mkWorkerInstance(workerData.getConf());
		if (syncContainerHbThread != null) {
			threads.add(syncContainerHbThread);
		}

		MetricReporter metricReporter = workerData.getMetricsReporter();
		boolean isMetricsEnable = ConfigExtension
				.isEnablePerformanceMetrics(workerData.getStormConf());
		metricReporter.setEnable(isMetricsEnable);
		metricReporter.start();
		LOG.info("Start metrics reporter, enable performance metrics: "
				+ isMetricsEnable);
		
		// create task heartbeat
		TaskHeartbeatRunable taskHB = new TaskHeartbeatRunable(workerData);
		AsyncLoopThread taskHBThread = new AsyncLoopThread(taskHB);
		threads.add(taskHBThread);

		// refresh hearbeat to Local dir
		RunnableCallback heartbeat_fn = new WorkerHeartbeatRunable(workerData);
		AsyncLoopThread hb = new AsyncLoopThread(heartbeat_fn, false, null,
				Thread.NORM_PRIORITY, true);
		threads.add(hb);

		// shutdown task callbacks
		List<TaskShutdownDameon> shutdowntasks = createTasks();
		workerData.setShutdownTasks(shutdowntasks);

		// start dispatcher
		dispatcher.start();

		return new WorkerShutdown(workerData, shutdowntasks, threads, metricReporter);

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
		
		w.redirectOutput();

		return w.execute();
	}

	public void redirectOutput(){

		if (System.getenv("REDIRECT") == null
				|| !System.getenv("REDIRECT").equals("true")) {
			return;
		}
		
		String DEFAULT_OUT_TARGET_FILE = JStormUtils.getLogFileName();
		if (DEFAULT_OUT_TARGET_FILE == null) {
			DEFAULT_OUT_TARGET_FILE = "/dev/null";
		} else {
			DEFAULT_OUT_TARGET_FILE += ".out";
		}
		
		String outputFile = ConfigExtension.getWorkerRedirectOutputFile(workerData.getStormConf());
		if (outputFile == null) {
			outputFile = DEFAULT_OUT_TARGET_FILE;
		}else {
			try {
				File file = new File(outputFile);
				if (file.exists() == false) {
					PathUtils.touch(outputFile);
				}else {
					if (file.isDirectory() == true) {
						LOG.warn("Failed to write " + outputFile);
						outputFile = DEFAULT_OUT_TARGET_FILE;
					}else if (file.canWrite() == false) {
						LOG.warn("Failed to write " + outputFile);
						outputFile = DEFAULT_OUT_TARGET_FILE;
					}
				}

			}catch(Exception e) {
				LOG.warn("Failed to touch " + outputFile, e);
				outputFile = DEFAULT_OUT_TARGET_FILE;
			}
		}

		try {
			JStormUtils.redirectOutput(outputFile);
		}catch(Exception e) {
			LOG.warn("Failed to redirect to " + outputFile, e);
		}

	}

	/**
	 * Have one problem if the worker's start parameter length is longer than
	 * 4096, ps -ef|grep com.alibaba.jstorm.daemon.worker.Worker can't find
	 * worker
	 * 
	 * @param port
	 */

	public static List<Integer> getOldPortPids(String port) {
		String currPid = JStormUtils.process_pid();

		List<Integer> ret = new ArrayList<Integer>();

		StringBuilder sb = new StringBuilder();

		sb.append("ps -Af ");
		// sb.append(" | grep ");
		// sb.append(Worker.class.getName());
		// sb.append(" |grep ");
		// sb.append(port);
		// sb.append(" |grep -v grep");

		try {
			LOG.info("Begin to execute " + sb.toString());
			Process process = JStormUtils.launch_process(sb.toString(),
					new HashMap<String, String>(), false);

			// Process process = Runtime.getRuntime().exec(sb.toString());

			InputStream stdin = process.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					stdin));

			JStormUtils.sleepMs(1000);

			// if (process.exitValue() != 0) {
			// LOG.info("Failed to execute " + sb.toString());
			// return null;
			// }

			String str;
			while ((str = reader.readLine()) != null) {
				if (StringUtils.isBlank(str)) {
					// LOG.info(str + " is Blank");
					continue;
				}

				// LOG.info("Output:" + str);
				if (str.contains(Worker.class.getName()) == false) {
					continue;
				} else if (str.contains(port) == false) {
					continue;
				}

				LOG.info("Find :" + str);

				String[] fields = StringUtils.split(str);

				boolean find = false;
				int i = 0;
				 for (; i < fields.length; i++) {
					 String field = fields[i];
					 LOG.debug("Filed, " + i+ ":" + field);
					 
					 if (field.contains(Worker.class.getName()) == true) {
						 if (i + 3 >= fields.length) {
							 LOG.info("Failed to find port ");
							 
						 }else if (fields[i + 3].equals(String.valueOf(port))) {
							 find = true;
						 }
						 
						 break;
					 }
				 }
				 
				 if (find == false) {
					 LOG.info("No old port worker");
					 continue;
				 }

				if (fields.length >= 2) {
					try {
						if (currPid.equals(fields[1])) {
							LOG.info("Skip kill myself");
							continue;
						}

						Integer pid = Integer.valueOf(fields[1]);

						LOG.info("Find one process :" + pid.toString());
						ret.add(pid);
					} catch (Exception e) {
						LOG.error(e.getMessage(), e);
						continue;
					}
				}

			}

			return ret;
		} catch (IOException e) {
			LOG.info("Failed to execute " + sb.toString());
			return ret;
		} catch (Exception e) {
			LOG.info(e.getCause(), e);
			return ret;
		}
	}

	public static void killOldWorker(String port) {

		List<Integer> oldPids = getOldPortPids(port);
		for (Integer pid : oldPids) {

			JStormUtils.kill(pid);
		}

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

		StringBuilder sb = new StringBuilder();

		try {
			String topology_id = args[0];
			String supervisor_id = args[1];
			String port_str = args[2];
			String worker_id = args[3];
			String jar_path = args[4];

			killOldWorker(port_str);

			Map conf = Utils.readStormConfig();
			StormConfig.validate_distributed_mode(conf);

			JStormServerUtils.startTaobaoJvmMonitor();
			
			sb.append("topologyId:" + topology_id + ", ");
			sb.append("port:" + port_str + ", ");
			sb.append("workerId:" + worker_id + ", ");
			sb.append("jar_path:" + jar_path + "\n");
			
			WorkerShutdown sd = mk_worker(conf, null, topology_id,
					supervisor_id, Integer.parseInt(port_str), worker_id,
					jar_path);
			sd.join();

			LOG.info("Successfully shutdown worker " + sb.toString());
		} catch (Throwable e) {
			String errMsg = "Failed to create worker, " + sb.toString();
			LOG.error(errMsg, e);
			JStormUtils.halt_process(-1, errMsg);
		}
	}

}
