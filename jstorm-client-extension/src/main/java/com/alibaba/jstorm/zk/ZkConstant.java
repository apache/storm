package com.alibaba.jstorm.zk;

public class ZkConstant {
	
	public static final String ZK_SEPERATOR = "/";

	public static final String ASSIGNMENTS_BAK = "assignments_bak";
	
	public static final String ASSIGNMENTS_BAK_SUBTREE;
	
	public static final String NIMBUS_SLAVE_ROOT = "nimbus_slave";
	
	public static final String NIMBUS_SLAVE_SUBTREE;
	
	static {
		ASSIGNMENTS_BAK_SUBTREE = ZK_SEPERATOR + ASSIGNMENTS_BAK;
		NIMBUS_SLAVE_SUBTREE = ZK_SEPERATOR + NIMBUS_SLAVE_ROOT;
	}
}
