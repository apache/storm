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

	private CgroupCenter center;

	private Hierarchy h;

	private CgroupCommon rootCgroup;

	private static final String JSTORM_CPU_HIERARCHY_DIR = "/cgroup/cpu";
	private static String root_dir;
	public CgroupManager(Map conf) {
		LOG.info("running on cgroup mode");
	
		// Cgconfig service is used to create the corresponding cpu hierarchy "/cgroup/cpu"
		root_dir = ConfigExtension.getCgroupRootDir(conf);
		if(root_dir == null)
				throw new RuntimeException(
					"Check configuration file. The supervisor.cgroup.rootdir is missing.");
		
		File file = new File(JSTORM_CPU_HIERARCHY_DIR + "/" + root_dir);
		if(!file.exists()) {		
		    LOG.error(JSTORM_CPU_HIERARCHY_DIR + "/" + root_dir + " is not existing.");
			throw new RuntimeException(
					"Check if cgconfig service starts or /etc/cgconfig.conf is consistent with configuration file.");
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
		cpuCore.setCpuShares(cpuNum * ONE_CPU_SLOT);
		StringBuilder sb = new StringBuilder();
		sb.append("cgexec -g cpu:").append(workerGroup.getName()).append(" ");
		return sb.toString();
	}

	public void shutDownWorker(String workerId, boolean isKilled) {
		CgroupCommon workerGroup = new CgroupCommon(workerId, h,
				this.rootCgroup);
		try {
			if (isKilled == false) {
				for (Integer pid : workerGroup.getTasks()) {
					JStormUtils.kill(pid);
				}
				JStormUtils.sleepMs(1500);
			}
			center.delete(workerGroup);
		}catch(Exception e) {
			LOG.info("No task of " + workerId);
		}
		
	}

	public void close() throws IOException {
		this.center.delete(this.rootCgroup);
	}

	private void prepareSubSystem() {
			h = center.busy(SubSystemType.cpu);
			if (h == null) {
				Set<SubSystemType> types = new HashSet<SubSystemType>();
				types.add(SubSystemType.cpu);
			h = new Hierarchy(JSTORM_HIERARCHY_NAME, types,JSTORM_CPU_HIERARCHY_DIR);
			}
		rootCgroup = new CgroupCommon(root_dir, h, h.getRootCgroups());
	}
}
