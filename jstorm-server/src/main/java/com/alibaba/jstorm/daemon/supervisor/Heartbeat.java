package com.alibaba.jstorm.daemon.supervisor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.resource.ResourceType;
import com.alibaba.jstorm.resource.SharedResourcePool;
import com.alibaba.jstorm.resource.SlotResourcePool;
import com.alibaba.jstorm.utils.JStormServerConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.alibaba.jstorm.utils.TimeUtils;

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
	    
        String myHostName = ConfigExtension.getSupervisorHost(conf);
        if (myHostName == null) {
            myHostName = NetWorkUtils.hostname();
        }
        
        if (ConfigExtension.isSupervisorUseIp(conf)) {
        	myHostName = NetWorkUtils.ip();
        }
	    
		this.stormClusterState = stormClusterState;
		this.supervisorId = supervisorId;
		this.conf = conf;
		this.myHostName = myHostName;
		this.startTime = TimeUtils.current_time_secs();
		this.active = active;
		this.frequence = JStormUtils.parseInt(conf
				.get(Config.SUPERVISOR_HEARTBEAT_FREQUENCY_SECS));

		initSupervisorInfo(conf);
	}

	private int getCpuSlotNum(Map conf) {
		int weight = ConfigExtension.getCpuSlotPerWeight(conf);
		Integer object = JStormServerConfig.getSupervisorCpuSlotNum(conf);
		if (object != null && JStormUtils.parseInt(object) > 0) {
			return JStormUtils.parseInt(object);
		}

		int sysCpuNum = Runtime.getRuntime().availableProcessors();
		if (sysCpuNum <= CPU_THREADHOLD) {
			return CPU_THREADHOLD * weight;
		} else {
			return (sysCpuNum - 1) * weight;
		}
	}

	private SharedResourcePool initCpuResourcePool(Map conf) {
		int slotNum = getCpuSlotNum(conf);

		SharedResourcePool ret = new SharedResourcePool(ResourceType.CPU,
				slotNum);

		return ret;
	}

	private int getMemSlotNum(Map conf) {
		Integer object = JStormServerConfig.getSupervisorMemSlotNum(conf);
		if (object != null && JStormUtils.parseInt(object) > 0) {
			return JStormUtils.parseInt(object);
		}

		Long physicalMemSize = JStormUtils.getPhysicMemorySize();
		if (physicalMemSize == null) {
			throw new RuntimeException(
					"Failed to get system physical memory, please set by manual");
		}
		LOG.info("Get system memory size :" + physicalMemSize);

		long availablePhyMemSize = 0;
		if (physicalMemSize <= MEM_THREADHOLD) {
			availablePhyMemSize = MEM_THREADHOLD;
		} else {
			availablePhyMemSize = Math.min((long) (physicalMemSize * 0.8),
					physicalMemSize - 4 * JStormUtils.SIZE_1_G);
		}

		long memSlotSize = ConfigExtension.getMemSlotSize(conf);

		int ret = (int) (availablePhyMemSize / memSlotSize);
		if (ret <= 0) {
			ret = 1;
		}
		return ret;

	}

	private SharedResourcePool initMemResourcePool(Map conf) {
		int slotNum = getMemSlotNum(conf);

		SharedResourcePool ret = new SharedResourcePool(ResourceType.MEM,
				slotNum);

		return ret;
	}

	private SlotResourcePool<String> initDiskResourcePool(Map conf) {
		List<String> object = JStormServerConfig.getSupervisorDiskSlots(conf);

		Set<String> slotsSet = new TreeSet<String>();
		if (object != null && object instanceof List) {
			slotsSet = JStormUtils.listToSet((List) object);
		} else {
			// use default disk slot
			// $(storm.local.dir)/workers/sharedata
			try {
				String workerDataDir = StormConfig
						.default_worker_shared_dir(conf);
				slotsSet.add(workerDataDir);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

		}

		return new SlotResourcePool<String>(ResourceType.DISK, slotsSet);
	}

	private SlotResourcePool<Integer> initNetworkPool(Map conf) {
		List<Integer> portList = (List<Integer>) conf
				.get(Config.SUPERVISOR_SLOTS_PORTS);

		if (!StormConfig.local_mode(conf)) {
			Set<Integer> ports = JStormUtils.listToSet(portList);
			return new SlotResourcePool<Integer>(ResourceType.NET, ports);
		} else {
			List<Integer> result = new ArrayList<Integer>();
			int port = portList.get(0);
			result.add(port);
			Set<Integer> ports = JStormUtils.listToSet(result);
			return new SlotResourcePool<Integer>(ResourceType.NET, ports);
		}
	}

	private void initSupervisorInfo(Map conf) {
		supervisorInfo = new SupervisorInfo(myHostName);

		supervisorInfo.setCpuPool(initCpuResourcePool(conf));
		supervisorInfo.setMemPool(initMemResourcePool(conf));
		supervisorInfo.setDiskPool(initDiskResourcePool(conf));
		supervisorInfo.setNetPool(initNetworkPool(conf));
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
