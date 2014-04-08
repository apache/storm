package com.alibaba.jstorm.container.cgroup.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.jstorm.container.CgroupUtils;
import com.alibaba.jstorm.container.Constants;
import com.alibaba.jstorm.container.SubSystemType;

public class NetPrioCore implements CgroupCore {

	public static final String NET_PRIO_PRIOIDX = "/net_prio.prioidx";
	public static final String NET_PRIO_IFPRIOMAP = "/net_prio.ifpriomap";

	private final String dir;

	public NetPrioCore(String dir) {
		this.dir = dir;
	}

	@Override
	public SubSystemType getType() {
		// TODO Auto-generated method stub
		return SubSystemType.net_prio;
	}

	public int getPrioId() throws IOException {
		return Integer.parseInt(CgroupUtils.readFileByLine(
				Constants.getDir(this.dir, NET_PRIO_PRIOIDX)).get(0));
	}

	public void setIfPrioMap(String iface, int priority) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append(iface);
		sb.append(' ');
		sb.append(priority);
		CgroupUtils.writeFileByLine(
				Constants.getDir(this.dir, NET_PRIO_IFPRIOMAP), sb.toString());
	}

	public Map<String, Integer> getIfPrioMap() throws IOException {
		Map<String, Integer> result = new HashMap<String, Integer>();
		List<String> strs = CgroupUtils.readFileByLine(Constants.getDir(
				this.dir, NET_PRIO_IFPRIOMAP));
		for (String str : strs) {
			String[] strArgs = str.split(" ");
			result.put(strArgs[0], Integer.valueOf(strArgs[1]));
		}
		return result;
	}

}
