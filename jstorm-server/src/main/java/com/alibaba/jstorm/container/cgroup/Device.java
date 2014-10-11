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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + major;
		result = prime * result + minor;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Device other = (Device) obj;
		if (major != other.major)
			return false;
		if (minor != other.minor)
			return false;
		return true;
	}

	

}
