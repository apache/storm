package com.alibaba.jstorm.container.cgroup.core;

import java.io.IOException;

import com.alibaba.jstorm.container.CgroupUtils;
import com.alibaba.jstorm.container.Constants;
import com.alibaba.jstorm.container.SubSystemType;
import com.alibaba.jstorm.container.cgroup.Device;

public class NetClsCore implements CgroupCore {

	public static final String NET_CLS_CLASSID = "/net_cls.classid";

	private final String dir;

	public NetClsCore(String dir) {
		this.dir = dir;
	}

	@Override
	public SubSystemType getType() {
		// TODO Auto-generated method stub
		return SubSystemType.net_cls;
	}

	private StringBuilder toHex(int num) {
		String hex = num + "";
		StringBuilder sb = new StringBuilder();
		int l = hex.length();
		if (l > 4) {
			hex = hex.substring(l - 4 - 1, l);
		}
		for (; l < 4; l++) {
			sb.append('0');
		}
		sb.append(hex);
		return sb;
	}

	public void setClassId(int major, int minor) throws IOException {
		StringBuilder sb = new StringBuilder("0x");
		sb.append(toHex(major));
		sb.append(toHex(minor));
		CgroupUtils.writeFileByLine(
				Constants.getDir(this.dir, NET_CLS_CLASSID), sb.toString());
	}

	public Device getClassId() throws IOException {
		String output = CgroupUtils.readFileByLine(
				Constants.getDir(this.dir, NET_CLS_CLASSID)).get(0);
		output = Integer.toHexString(Integer.parseInt(output));
		int major = Integer.parseInt(output.substring(0, output.length() - 4));
		int minor = Integer.parseInt(output.substring(output.length() - 4));
		return new Device(major, minor);
	}

}
