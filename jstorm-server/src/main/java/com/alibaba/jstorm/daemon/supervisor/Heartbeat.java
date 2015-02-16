package com.alibaba.jstorm.daemon.supervisor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.alibaba.jstorm.utils.JStormServerUtils;

/**
 * supervisor Heartbeat, just write SupervisorInfo to ZK
 */
class Heartbeat extends RunnableCallback {

	private static Logger LOG = Logger.getLogger(Heartbeat.class);

	private static final int CPU_THREADHOLD = 4;
	private static final long MEM_THREADHOLD = 8 * JStormUtils.SIZE_1_G;

	private Map<Object, Object> conf;

	private StormClusterState stormClusterState;

	private String supervisorId;

	private String myHostName;

	private final int startTime;

	private final int frequence;

	private SupervisorInfo supervisorInfo;

	/**
	 * @param conf
	 * @param stormClusterState
	 * @param supervisorId
	 * @param myHostName
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Heartbeat(Map conf, StormClusterState stormClusterState,
			String supervisorId, AtomicBoolean active) {

		String myHostName = JStormServerUtils.getHostName(conf);

		this.stormClusterState = stormClusterState;
		this.supervisorId = supervisorId;
		this.conf = conf;
		this.myHostName = myHostName;
		this.startTime = TimeUtils.current_time_secs();
		this.active = active;
		this.frequence = JStormUtils.parseInt(conf
				.get(Config.SUPERVISOR_HEARTBEAT_FREQUENCY_SECS));

		initSupervisorInfo(conf);
		
		LOG.info("Successfully init supervisor heartbeat thread, " + supervisorInfo);
	}

	private void initSupervisorInfo(Map conf) {
		List<Integer> portList = JStormUtils.getSupervisorPortList(conf);

		if (!StormConfig.local_mode(conf)) {
			Set<Integer> ports = JStormUtils.listToSet(portList);
			supervisorInfo = new SupervisorInfo(myHostName, supervisorId, ports);
		} else {
			Set<Integer> ports = JStormUtils.listToSet(portList.subList(0, 1));
			supervisorInfo = new SupervisorInfo(myHostName, supervisorId, ports);
		}
	}

	@SuppressWarnings("unchecked")
	public void update() {

		supervisorInfo.setTimeSecs(TimeUtils.current_time_secs());
		supervisorInfo
				.setUptimeSecs((int) (TimeUtils.current_time_secs() - startTime));

		try {
			stormClusterState
					.supervisor_heartbeat(supervisorId, supervisorInfo);
		} catch (Exception e) {
			LOG.error("Failed to update SupervisorInfo to ZK", e);

		}
	}

	private AtomicBoolean active = null;

	private Integer result;

	@Override
	public Object getResult() {
		return result;
	}

	@Override
	public void run() {
		update();
		if (active.get()) {
			result = frequence;
		} else {
			result = -1;
		}
	}

	public int getStartTime() {
		return startTime;
	}

	public SupervisorInfo getSupervisorInfo() {
		return supervisorInfo;
	}

}
