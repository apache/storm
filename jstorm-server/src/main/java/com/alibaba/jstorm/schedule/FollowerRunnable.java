package com.alibaba.jstorm.schedule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.task.Assignment;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.alibaba.jstorm.utils.PathUtils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;

public class FollowerRunnable implements Runnable {

	private static final Logger LOG = Logger.getLogger(FollowerRunnable.class);

	private NimbusData data;

	private LeaderSelector leaderSelector;

	private int sleepTime;

	private CuratorFramework zkobj;

	private volatile boolean state = true;

	private RunnableCallback callback;

	@SuppressWarnings("unchecked")
	public FollowerRunnable(NimbusData data, LeaderSelector leaderSelector,
			int sleepTime) {
		this.data = data;
		this.leaderSelector = leaderSelector;
		this.sleepTime = sleepTime;
		try {
			data.getStormClusterState().register_nimbus_host(
					NetWorkUtils.hostname()
							+ "_pid_" + JStormUtils.process_pid());
		} catch (Exception e) {
			LOG.error("register nimbus host fail!", e);
			throw new RuntimeException();
		}
		callback = new RunnableCallback() {
			@Override
			public void run() {
				check();
			}
		};
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		LOG.info("Follower Thread starts!");
		while (state) {
			try {
				Thread.sleep(sleepTime);
				if (leaderSelector.hasLeadership())
					continue;
				check();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				continue;
			} catch (Exception e) {
				if (state) {
					LOG.error("Unknow exception ", e);
				}
			}
		}
		zkobj.close();
		LOG.info("Follower Thread has closed!");
	}

	public void clean() {
		state = false;
	}

	private void check() {

		StormClusterState clusterState = data.getStormClusterState();

		try {
			String master_stormdist_root = StormConfig.masterStormdistRoot(data
					.getConf());

			List<String> code_ids = PathUtils
					.read_dir_contents(master_stormdist_root);

			List<String> assignments_ids = clusterState.assignments(callback);

			List<String> done_ids = new ArrayList<String>();

			for (String id : code_ids) {
				if (assignments_ids.contains(id)) {
					done_ids.add(id);
				}
			}

			for (String id : done_ids) {
				assignments_ids.remove(id);
				code_ids.remove(id);
			}

			for (String topologyId : code_ids) {
				deleteLocalTopology(topologyId);
			}

			for (String id : assignments_ids) {
				Assignment assignment = clusterState.assignment_info(id, null);
				downloadCodeFromMaster(assignment, id);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("Get stormdist dir error!");
			e.printStackTrace();
			return;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("Check error!");
			e.printStackTrace();
			return;
		}
	}

	private void deleteLocalTopology(String topologyId) throws IOException {
		String dir_to_delete = StormConfig.masterStormdistRoot(data.getConf(),
				topologyId);
		try {
			PathUtils.rmr(dir_to_delete);
			LOG.info("delete:" + dir_to_delete + "successfully!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("delete:" + dir_to_delete + "fail!");
			e.printStackTrace();
		}
	}

	private void downloadCodeFromMaster(Assignment assignment, String topologyId)
			throws IOException, TException {
		try {
			String localRoot = StormConfig.masterStormdistRoot(data.getConf(),
					topologyId);
			String masterCodeDir = assignment.getMasterCodeDir();
			JStormServerUtils.downloadCodeFromMaster(data.getConf(), localRoot,
					masterCodeDir);
		} catch (TException e) {
			// TODO Auto-generated catch block
			LOG.error(e + " downloadStormCode failed " + "topologyId:"
					+ topologyId + "masterCodeDir:"
					+ assignment.getMasterCodeDir());
			throw e;
		}
		LOG.info("Finished downloading code for topology id " + topologyId
				+ " from " + assignment.getMasterCodeDir());
	}


}
