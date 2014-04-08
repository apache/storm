package com.alibaba.jstorm.daemon.supervisor;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.container.CgroupCenter;
import com.alibaba.jstorm.container.Hierarchy;
import com.alibaba.jstorm.container.SubSystemType;
import com.alibaba.jstorm.container.SystemOperation;
import com.alibaba.jstorm.container.cgroup.CgroupCommon;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.container.cgroup.core.CgroupCore;
import com.alibaba.jstorm.container.cgroup.core.CpuCore;

public class CgroupManager {

	public static final Logger LOG = Logger.getLogger(CgroupManager.class);

	public static final String JSTORM_HIERARCHY_NAME = "jstorm_cpu";

	public static final int ONE_CPU_SLOT = 1024;

	public final int CPU_SLOT_PER_WEIGHT;

	private CgroupCenter center;

	private Hierarchy h;

	private CgroupCommon rootCgroup;

	public CgroupManager(Map conf) {
		LOG.info("running on cgroup mode");
		this.CPU_SLOT_PER_WEIGHT = ConfigExtension.getCpuSlotPerWeight(conf);
		try {
			if (!SystemOperation.isRoot())
				throw new RuntimeException(
						"If you want to use cgroup, please start supervisor on root");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("check root error", e);
			throw new RuntimeException(e);
		}
		center = CgroupCenter.getInstance();
		if (center == null)
			throw new RuntimeException(
					"Cgroup error, please check /proc/cgroups");
		this.prepareSubSystem();
	}

	public String startNewWorker(int cpuNum, String workerId)
			throws SecurityException, IOException {
		CgroupCommon workerGroup = new CgroupCommon(workerId, h,
				this.rootCgroup);
		this.center.create(workerGroup);
		CgroupCore cpu = workerGroup.getCores().get(SubSystemType.cpu);
		CpuCore cpuCore = (CpuCore) cpu;
		cpuCore.setCpuShares(cpuNum * ONE_CPU_SLOT / CPU_SLOT_PER_WEIGHT);
		StringBuilder sb = new StringBuilder();
		sb.append("cgexec -g cpu:").append(workerGroup.getName()).append(" ");
		return sb.toString();
	}

	public void shutDownWorker(String workerId) throws IOException {
		CgroupCommon workerGroup = new CgroupCommon(workerId, h,
				this.rootCgroup);
		for (Integer pid : workerGroup.getTasks()) {
			JStormUtils.process_killed(pid);
		}
		JStormUtils.sleepMs(1500);
		center.delete(workerGroup);
	}

	public void close() throws IOException {
		this.center.delete(this.rootCgroup);
	}

	private void prepareSubSystem() {
		try {
			h = center.busy(SubSystemType.cpu);
			if (h == null) {
				Set<SubSystemType> types = new HashSet<SubSystemType>();
				types.add(SubSystemType.cpu);
				h = new Hierarchy(JSTORM_HIERARCHY_NAME, types,
						"/jstorm/cgroup/cpu");
				center.mount(h);
			}
			File file = new File(h.getDir() + "/worker_root");
			rootCgroup = new CgroupCommon("worker_root", h, h.getRootCgroups());
			if (!file.exists()) {
				center.create(rootCgroup);
			}
		} catch (IOException e) {
			LOG.error("prepare subsystem error!", e);
			throw new RuntimeException(e);
		}
	}
}
