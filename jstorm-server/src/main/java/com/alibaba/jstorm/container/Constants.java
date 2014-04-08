package com.alibaba.jstorm.container;

public class Constants {

	public static final String CGROUP_STATUS_FILE = "/proc/cgroups";

	public static final String MOUNT_STATUS_FILE = "/proc/mounts";
	
	public static String getDir(String dir, String constant) {
		return dir + constant;
	}

}
