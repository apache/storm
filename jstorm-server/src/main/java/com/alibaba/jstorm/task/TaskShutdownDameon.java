package com.alibaba.jstorm.task;

import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeatRunable;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * shutdown one task
 * 
 * @author yannian/Longda
 * 
 */
public class TaskShutdownDameon implements ShutdownableDameon {
	private static Logger LOG = Logger.getLogger(TaskShutdownDameon.class);

	public static final byte QUIT_MSG = (byte) 0xff;

	private TaskStatus taskStatus;
	private String topology_id;
	private Integer task_id;
	private List<AsyncLoopThread> all_threads;
	private StormClusterState zkCluster;
	private Object task_obj;
	private boolean isClosed = false;

	public TaskShutdownDameon(TaskStatus taskStatus, String topology_id,
			Integer task_id, List<AsyncLoopThread> all_threads,
			StormClusterState zkCluster, Object task_obj) {
		this.taskStatus = taskStatus;
		this.topology_id = topology_id;
		this.task_id = task_id;
		this.all_threads = all_threads;
		this.zkCluster = zkCluster;
		this.task_obj = task_obj;

	}

	@Override
	public void shutdown() {
		synchronized (this) {
			if (isClosed == true) {
				return ;
			}
			isClosed = true;
		}
		
		LOG.info("Begin to shut down task " + topology_id + ":" + task_id);

		// all thread will check the taskStatus
		// once it has been set SHUTDOWN, it will quit
		taskStatus.setStatus(TaskStatus.SHUTDOWN);
		
		// waiting 100ms for executor thread shutting it's own
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
		}

		for (AsyncLoopThread thr : all_threads) {
			LOG.info("Begin to shutdown " + thr.getThread().getName());
			thr.cleanup();
			JStormUtils.sleepMs(10);
			thr.interrupt();
//			try {
//				//thr.join();
//				thr.getThread().stop(new RuntimeException());
//			} catch (Throwable e) {
//			}
			LOG.info("Successfully shutdown " + thr.getThread().getName());
		}
		
		closeComponent(task_obj);

		try {
			TaskHeartbeatRunable.unregisterTaskStats(task_id);
			zkCluster.remove_task_heartbeat(topology_id, task_id);
			zkCluster.disconnect();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.info(e);
		}

		LOG.info("Successfully shutdown task " + topology_id + ":" + task_id);

	}

	public void join() throws InterruptedException {
		for (AsyncLoopThread t : all_threads) {
			t.join();
		}
	}

	private void closeComponent(Object _task_obj) {
		if (_task_obj instanceof IBolt) {
			((IBolt) _task_obj).cleanup();
		}

		if (_task_obj instanceof ISpout) {
			((ISpout) _task_obj).close();
		}
	}

	@Override
	public boolean waiting() {
		return taskStatus.isRun();
	}

	public void deactive() {

		if (task_obj instanceof ISpout) {
			taskStatus.setStatus(TaskStatus.PAUSE);
			WorkerClassLoader.switchThreadContext();

			try {
				((ISpout) task_obj).deactivate();
			} finally {
				WorkerClassLoader.restoreThreadContext();
			}
		}

	}

	public void active() {
		if (task_obj instanceof ISpout) {
			taskStatus.setStatus(TaskStatus.RUN);
			WorkerClassLoader.switchThreadContext();
			try {
				((ISpout) task_obj).activate();
			} finally {
				WorkerClassLoader.restoreThreadContext();
			}
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		shutdown();
	}
}
