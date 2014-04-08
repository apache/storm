package com.alibaba.jstorm.container;

public enum SubSystemType {

	// net_cls,ns is not supposted in ubuntu
	blkio, cpu, cpuacct, cpuset, devices, freezer, memory, perf_event, net_cls, net_prio;

	public static SubSystemType getSubSystem(String str) {
		if (str.equals("blkio"))
			return blkio;
		else if (str.equals("cpu"))
			return cpu;
		else if (str.equals("cpuacct"))
			return cpuacct;
		else if (str.equals("cpuset"))
			return cpuset;
		else if (str.equals("devices"))
			return devices;
		else if (str.equals("freezer"))
			return freezer;
		else if (str.equals("memory"))
			return memory;
		else if (str.equals("perf_event"))
			return perf_event;
		else if (str.equals("net_cls"))
			return net_cls;
		else if (str.equals("net_prio"))
			return net_prio;
		return null;
	}
}
