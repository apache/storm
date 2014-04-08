package com.alibaba.jstorm.container.cgroup.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.jstorm.container.CgroupUtils;
import com.alibaba.jstorm.container.Constants;
import com.alibaba.jstorm.container.SubSystemType;

public class CpuacctCore implements CgroupCore {

	public static final String CPUACCT_USAGE = "/cpuacct.usage";
	public static final String CPUACCT_STAT = "/cpuacct.stat";
	public static final String CPUACCT_USAGE_PERCPU = "/cpuacct.usage_percpu";

	private final String dir;
	
	public CpuacctCore(String dir) {
		this.dir = dir;
	}

	@Override
	public SubSystemType getType() {
		// TODO Auto-generated method stub
		return SubSystemType.cpuacct;
	}

	public Long getCpuUsage() throws IOException {
		return Long.parseLong(CgroupUtils.readFileByLine(
				Constants.getDir(this.dir, CPUACCT_USAGE)).get(0));
	}

	public Map<StatType, Long> getCpuStat() throws IOException {
		List<String> strs = CgroupUtils.readFileByLine(Constants.getDir(
				this.dir, CPUACCT_STAT));
		Map<StatType, Long> result = new HashMap<StatType, Long>();
		result.put(StatType.user, Long.parseLong(strs.get(0).split(" ")[1]));
		result.put(StatType.system, Long.parseLong(strs.get(1).split(" ")[1]));
		return result;
	}

	public Long[] getPerCpuUsage() throws IOException {
		String str = CgroupUtils.readFileByLine(
				Constants.getDir(this.dir, CPUACCT_USAGE_PERCPU)).get(0);
		String[] strArgs = str.split(" ");
		Long[] result = new Long[strArgs.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = Long.parseLong(strArgs[i]);
		}
		return result;
	}

	public enum StatType {
		user, system;
	}

}
