package com.alibaba.jstorm.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.messaging.IContext;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;
import clojure.lang.Atom;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormZkClusterState;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.daemon.worker.WorkerHaltRunable;
import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.task.error.TaskReportError;
import com.alibaba.jstorm.task.error.TaskReportErrorAndDie;
import com.alibaba.jstorm.task.execute.BaseExecutors;
import com.alibaba.jstorm.task.execute.BoltExecutors;
import com.alibaba.jstorm.task.execute.spout.MultipleThreadSpoutExecutors;
import com.alibaba.jstorm.task.execute.spout.SingleThreadSpoutExecutors;
import com.alibaba.jstorm.task.execute.spout.SpoutExecutors;
import com.alibaba.jstorm.task.group.MkGrouper;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeatRunable;
import com.alibaba.jstorm.task.heartbeat.TaskStats;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Task instance
 * 
 * @author yannian/Longda
 * 
 */
public class Task {

	private final static Logger LOG = Logger.getLogger(Task.class);

	private Map<Object, Object> stormConf;

	private TopologyContext topologyContext;
	private TopologyContext userContext;
	private String topologyid;
	private IContext context;

	private TaskTransfer taskTransfer;
	private Map<Integer, DisruptorQueue> innerTaskTransfer;
	private Map<Integer, DisruptorQueue> deserializeQueues;
	private WorkerHaltRunable workHalt;

	private Integer taskid;
	private String componentid;
	private volatile TaskStatus taskStatus;
	private Atom openOrPrepareWasCalled;
	// running time counter
	private UptimeComputer uptime = new UptimeComputer();

	private StormClusterState zkCluster;
	private Object taskObj;
	private CommonStatsRolling taskStats;
	private WorkerData workerData;
	private String componentType; //"spout" or "bolt"

	@SuppressWarnings("rawtypes")
	public Task(WorkerData workerData, int taskId) throws Exception {
		openOrPrepareWasCalled = new Atom(Boolean.valueOf(false));

		this.workerData = workerData;
		this.topologyContext = workerData.getContextMaker()
				.makeTopologyContext(workerData.getSysTopology(), taskId,
						openOrPrepareWasCalled);
		this.userContext = workerData.getContextMaker().makeTopologyContext(
				workerData.getRawTopology(), taskId, openOrPrepareWasCalled);
		this.taskid = taskId;
		this.componentid = topologyContext.getThisComponentId();

		this.taskStatus = new TaskStatus();
		this.taskTransfer = getSendingTransfer(workerData);
		this.innerTaskTransfer = workerData.getInnerTaskTransfer();
		this.deserializeQueues = workerData.getDeserializeQueues();
		this.topologyid = workerData.getTopologyId();
		this.context = workerData.getContext();
		this.workHalt = workerData.getWorkHalt();
		this.zkCluster = new StormZkClusterState(workerData.getZkClusterstate());

		this.stormConf = Common.component_conf(workerData.getStormConf(),
				topologyContext, componentid);

		WorkerClassLoader.switchThreadContext();
		// get real task object -- spout/bolt/spoutspec
		this.taskObj = Common.get_task_object(topologyContext.getRawTopology(),
				componentid, WorkerClassLoader.getInstance());
		WorkerClassLoader.restoreThreadContext();
		int samplerate = StormConfig.sampling_rate(stormConf);
		this.taskStats = new CommonStatsRolling(samplerate);

		LOG.info("Loading task " + componentid + ":" + taskid);
	}
	
	private void setComponentType() {
		if (taskObj instanceof IBolt) {
			componentType = "bolt";
		} else if (taskObj instanceof ISpout) {
			componentType = "spout";
		}
	}

	private TaskSendTargets makeSendTargets() {
		String component = topologyContext.getThisComponentId();

		// get current task's output
		// <Stream_id,<component, Grouping>>
		Map<String, Map<String, MkGrouper>> streamComponentGrouper = Common
				.outbound_components(topologyContext, workerData);

		Map<Integer, String> task2Component = topologyContext
				.getTaskToComponent();
		Map<String, List<Integer>> component2Tasks = JStormUtils
				.reverse_map(task2Component);

		return new TaskSendTargets(stormConf, component,
				streamComponentGrouper, topologyContext, component2Tasks,
				taskStats);
	}

	private TaskTransfer getSendingTransfer(WorkerData workerData) {

		// sending tuple's serializer
		KryoTupleSerializer serializer = new KryoTupleSerializer(
				workerData.getStormConf(), topologyContext);

		String taskName = JStormServerUtils.getName(componentid, taskid);
		// Task sending all tuples through this Object
		return new TaskTransfer(taskName, serializer, taskStatus, workerData);
	}

	public TaskSendTargets echoToSystemBolt() {
		// send "startup" tuple to system bolt
		List<Object> msg = new ArrayList<Object>();
		msg.add("startup");

		// create task receive object
		TaskSendTargets sendTargets = makeSendTargets();
		UnanchoredSend.send(topologyContext, sendTargets, taskTransfer,
				Common.SYSTEM_STREAM_ID, msg);

		return sendTargets;
	}

	public boolean isSingleThread(Map conf) {
		boolean isOnePending = JStormServerUtils.isOnePending(conf);
		if (isOnePending == true) {
			return true;
		}

		return ConfigExtension.isSpoutSingleThread(conf);
	}

	public RunnableCallback mk_executors(DisruptorQueue deserializeQueue,
			TaskSendTargets sendTargets, ITaskReportErr report_error) {

		if (taskObj instanceof IBolt) {
			return new BoltExecutors((IBolt) taskObj, taskTransfer,
					innerTaskTransfer, stormConf, deserializeQueue, sendTargets,
					taskStatus, topologyContext, userContext, taskStats,
					report_error);
		} else if (taskObj instanceof ISpout) {
			if (isSingleThread(stormConf) == true) {
				return new SingleThreadSpoutExecutors((ISpout) taskObj, taskTransfer,
						innerTaskTransfer, stormConf, deserializeQueue, sendTargets,
						taskStatus, topologyContext, userContext, taskStats,
						report_error);
			}else {
				return new MultipleThreadSpoutExecutors((ISpout) taskObj, taskTransfer,
						innerTaskTransfer, stormConf, deserializeQueue, sendTargets,
						taskStatus, topologyContext, userContext, taskStats,
						report_error);
			}
		}

		return null;
	}

	/**
	 * create executor to receive tuples and run bolt/spout execute function
	 * 
	 * @param puller
	 * @param sendTargets
	 * @return
	 */
	private RunnableCallback mkExecutor(DisruptorQueue deserializeQueue,
			TaskSendTargets sendTargets) {
		// create report error callback,
		// in fact it is storm_cluster.report-task-error
		ITaskReportErr reportError = new TaskReportError(zkCluster, topologyid,
				taskid);

		// report error and halt worker
		TaskReportErrorAndDie reportErrorDie = new TaskReportErrorAndDie(
				reportError, workHalt);

		return mk_executors(deserializeQueue, sendTargets, reportErrorDie);
	}

	public DisruptorQueue registerDisruptorQueue() {
		int queueSize = JStormUtils.parseInt(
				stormConf.get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE), 256);
		
		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) stormConf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		DisruptorQueue queue = DisruptorQueue.mkInstance("TaskDeserialize", ProducerType.SINGLE,
				queueSize, waitStrategy);

		deserializeQueues.put(taskid, queue);

		return queue;
	}

	public TaskShutdownDameon execute() throws Exception {
		setComponentType();

		DisruptorQueue deserializeQueue = registerDisruptorQueue();

		TaskSendTargets sendTargets = echoToSystemBolt();

		// create thread to get tuple from zeroMQ,
		// and pass the tuple to bolt/spout
		RunnableCallback baseExecutor = mkExecutor(deserializeQueue, sendTargets);
		AsyncLoopThread executor_threads = new AsyncLoopThread(baseExecutor,
				false, Thread.MAX_PRIORITY, true);

		List<AsyncLoopThread> allThreads = new ArrayList<AsyncLoopThread>();
		allThreads.add(executor_threads);
		
		TaskHeartbeatRunable.registerTaskStats(taskid, new TaskStats(componentType, taskStats));
		LOG.info("Finished loading task " + componentid + ":" + taskid);

		return getShutdown(allThreads, deserializeQueue, baseExecutor);
	}

	public TaskShutdownDameon getShutdown(List<AsyncLoopThread> allThreads,
			DisruptorQueue deserializeQueue, RunnableCallback baseExecutor) {

		AsyncLoopThread ackerThread = null;
		if (baseExecutor instanceof SpoutExecutors) {
			ackerThread = ((SpoutExecutors) baseExecutor).getAckerRunnableThread();
			
			if (ackerThread != null) {
				allThreads.add(ackerThread);
			}
		}
		AsyncLoopThread recvThread = ((BaseExecutors) baseExecutor).getDeserlizeThread();
		allThreads.add(recvThread);

		AsyncLoopThread serializeThread = taskTransfer.getSerializeThread();
		allThreads.add(serializeThread);

		TaskShutdownDameon shutdown = new TaskShutdownDameon(taskStatus,
				topologyid, taskid, allThreads, zkCluster, taskObj);

		return shutdown;
	}

	public static TaskShutdownDameon mk_task(WorkerData workerData, int taskId)
			throws Exception {

		Task t = new Task(workerData, taskId);

		return t.execute();
	}

}
