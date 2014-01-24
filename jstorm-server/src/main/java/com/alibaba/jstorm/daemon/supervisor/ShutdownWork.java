package com.alibaba.jstorm.daemon.supervisor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.worker.ProcessSimulator;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;

public class ShutdownWork extends RunnableCallback {

	private static Logger LOG = Logger.getLogger(ShutdownWork.class);

	/**
	 * shutdown the spec worker of the supervisor. and clean the local dir of
	 * workers
	 * 
	 * 
	 * @param conf
	 * @param supervisorId
	 * @param workerId
	 * @param workerThreadPidsAtom
	 * @param workerThreadPidsAtomReadLock
	 */
	public void shutWorker(Map conf, String supervisorId, String workerId,
			ConcurrentHashMap<String, String> workerThreadPids)
			throws IOException {

		LOG.info("Begin to shut down " + supervisorId + ":" + workerId);

		// STORM-LOCAL-DIR/workers/workerId/pids
		String workerPidPath = StormConfig.worker_pids_root(conf, workerId);

		List<String> pids = PathUtils.read_dir_contents(workerPidPath);

		String threadPid = workerThreadPids.get(workerId);

		if (threadPid != null) {
			ProcessSimulator.killProcess(threadPid);
		}

		for (String pid : pids) {
			JStormUtils.process_killed(Integer.parseInt(pid));
		}

		// wait all thread exit correctly
		try {
			Thread.sleep(1000 * 2);
		} catch (InterruptedException e) {

		}

		for (String pid : pids) {

			JStormUtils.ensure_process_killed(Integer.parseInt(pid));
			PathUtils.rmpath(StormConfig.worker_pid_path(conf, workerId, pid));

		}

		tryCleanupWorkerDir(conf, workerId);

		LOG.info("Successfully shut down " + supervisorId + ":" + workerId);
	}

	/**
	 * clean the directory , subdirectories of STORM-LOCAL-DIR/workers/workerId
	 * 
	 * 
	 * @param conf
	 * @param workerId
	 * @throws IOException
	 */
	public void tryCleanupWorkerDir(Map conf, String workerId) {
		try {
			// delete heartbeat dir LOCAL_DIR/workers/workid/heartbeats
			PathUtils.rmr(StormConfig.worker_heartbeats_root(conf, workerId));
			// delete pid dir, LOCAL_DIR/workers/workerid/pids
			PathUtils.rmr(StormConfig.worker_pids_root(conf, workerId));
			// delete workerid dir, LOCAL_DIR/worker/workerid
			PathUtils.rmr(StormConfig.worker_root(conf, workerId));
		} catch (Exception e) {
			LOG.warn(e + "Failed to cleanup worker " + workerId
					+ ". Will retry later");
		}
	}
}
