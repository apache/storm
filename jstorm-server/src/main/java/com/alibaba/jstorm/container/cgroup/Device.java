package com.alibaba.jstorm.container.cgroup;

public class Device {

	public final int major;
	public final int minor;

	public Device(int major, int minor) {
		this.major = major;
		this.minor = minor;
	}

	public Device(String str) {
		String[] strArgs = str.split(":");
		this.major = Integer.valueOf(strArgs[0]);
		this.minor = Integer.valueOf(strArgs[1]);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(major).append(":").append(minor);
		return sb.toString();
	}

	@Override
	public boolean equals(Object object) {
		if (object instanceof Device && ((Device) object).major == major
				&& ((Device) object).minor == minor)
			return true;
		return false;
	}

}
