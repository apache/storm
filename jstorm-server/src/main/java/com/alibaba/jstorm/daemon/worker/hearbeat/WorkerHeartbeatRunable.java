package com.alibaba.jstorm.daemon.worker.hearbeat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.LocalState;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.daemon.worker.WorkerHeartbeat;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * worker Heartbeat
 * 
 * @author yannian/Longda
 * 
 */
public class WorkerHeartbeatRunable extends RunnableCallback {
	private static Logger LOG = Logger.getLogger(WorkerHeartbeatRunable.class);

	private WorkerData workerData;

	private AtomicBoolean active;
	private Map<Object, Object> conf;
	private String worker_id;
	private Integer port;
	private String topologyId;
	private CopyOnWriteArraySet<Integer> task_ids;
	// private Object lock = new Object();

	private Integer frequence;

	public WorkerHeartbeatRunable(WorkerData workerData) {

		this.workerData = workerData;

		this.conf = workerData.getConf();
		this.worker_id = workerData.getWorkerId();
		this.port = workerData.getPort();
		this.topologyId = workerData.getTopologyId();
		this.task_ids = new CopyOnWriteArraySet<Integer>(
				workerData.getTaskids());
		this.active = workerData.getActive();

		String key = Config.WORKER_HEARTBEAT_FREQUENCY_SECS;
		frequence = JStormUtils.parseInt(conf.get(key), 10);
	}

	/**
	 * do hearbeat, update LocalState
	 * 
	 * @throws IOException
	 */

	public void doHeartbeat() throws IOException {

		int currtime = TimeUtils.current_time_secs();
		WorkerHeartbeat hb = new WorkerHeartbeat(currtime, topologyId,
				task_ids, port);

		LOG.debug("Doing heartbeat:" + worker_id + ",port:" + port + ",hb"
				+ hb.toString());

		LocalState state = StormConfig.worker_state(conf, worker_id);
		state.put(Common.LS_WORKER_HEARTBEAT, hb);

	}

	@Override
	public void run() {

		if (active.get() == false) {
			return;

		}
		try {
			doHeartbeat();
		} catch (IOException e) {
			LOG.error("work_heart_beat_fn fail", e);
			throw new RuntimeException(e);
		}

	}

	@Override
	public Object getResult() {
		if (this.active.get()) {
			return frequence;
		}
		return -1;
	}
}
