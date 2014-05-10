package com.alibaba.jstorm.daemon.worker;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.task.comm.VirtualPortDispatch;
import com.alibaba.jstorm.task.comm.VirtualPortShutdown;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * worker receive tuple dispatcher
 * 
 * @author yannian/Longda
 * 
 */
public class WorkerVirtualPort {

	private final static Logger LOG = Logger.getLogger(WorkerVirtualPort.class);

	@SuppressWarnings("rawtypes")
	private Map conf;
	private String supervisorId;
	private Integer port;
	private IContext context;
	private Set<Integer> taskIds;
	private String topologyId;
	private AtomicBoolean active;

	public WorkerVirtualPort(WorkerData workerData) {
		//
		// Map conf, String supervisor_id, String topologyId,
		// Integer port, Icontext context, Set<Integer> task_ids
		this.conf = workerData.getStormConf();
		this.supervisorId = workerData.getSupervisorId();
		this.port = workerData.getPort();
		this.context = workerData.getContext();
		this.taskIds = workerData.getTaskids();
		this.topologyId = workerData.getTopologyId();
		this.active = workerData.getActive();
	}

	public Shutdownable launch() throws InterruptedException {

		String msg = "Launching virtual port for supervisor";
		LOG.info(msg + ":" + supervisorId + " topologyId:" + topologyId + " port:"
				+ port);

		boolean islocal = StormConfig.local_mode(conf);

		RunnableCallback killfn = JStormUtils.getDefaultKillfn();

		IConnection recvConnection = context.bind(topologyId, port, true);

		RunnableCallback recvDispather = new VirtualPortDispatch(topologyId,
				context, recvConnection, taskIds, active);

		AsyncLoopThread vthread = new AsyncLoopThread(recvDispather, false,
				killfn, Thread.MAX_PRIORITY, true);

		LOG.info("Successfully " + msg + ":" + supervisorId + " topologyId:"
				+ topologyId + " port:" + port);

		return new VirtualPortShutdown(topologyId, context, vthread, port);

	}

}
