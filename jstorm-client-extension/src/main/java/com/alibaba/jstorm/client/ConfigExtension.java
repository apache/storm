package com.alibaba.jstorm.client;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;

import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.utils.JStormUtils;

public class ConfigExtension {
	/**
	 * if this configure has been set, the spout or bolt will log all receive
	 * tuples
	 * 
	 * topology.debug just for logging all sent tuples
	 */
	protected static final String TOPOLOGY_DEBUG_RECV_TUPLE = "topology.debug.recv.tuple";

	public static void setTopologyDebugRecvTuple(Map conf, boolean debug) {
		conf.put(TOPOLOGY_DEBUG_RECV_TUPLE, Boolean.valueOf(debug));
	}

	public static Boolean isTopologyDebugRecvTuple(Map conf) {
		return JStormUtils.parseBoolean(conf.get(TOPOLOGY_DEBUG_RECV_TUPLE),
				false);
	}

	/**
	 * Whether or not should the messaging transfer use disruptor, By default
	 * enable
	 */
	protected static final String NETTY_ENABLE_DISRUPTOR_QUEUE = "storm.messaging.netty.disruptor";

	public static void setNettyEnableDisruptor(Map conf, boolean enable) {
		conf.put(NETTY_ENABLE_DISRUPTOR_QUEUE, Boolean.valueOf(enable));
	}

	public static Boolean isNettyEnableDisruptor(Map conf) {
		return JStormUtils.parseBoolean(conf.get(NETTY_ENABLE_DISRUPTOR_QUEUE),
				true);
	}

	/**
	 * Worker gc parameter
	 * 
	 * 
	 */
	protected static final String WORKER_GC_CHILDOPTS = "worker.gc.childopts";

	public static void setWorkerGc(Map conf, String gc) {
		conf.put(WORKER_GC_CHILDOPTS, gc);
	}

	public static String getWorkerGc(Map conf) {
		return (String) conf.get(WORKER_GC_CHILDOPTS);
	}

	/**
	 * Usually, spout finish prepare before bolt, so spout need wait several
	 * seconds so that bolt finish preparation
	 * 
	 * By default, the setting is 30 seconds
	 */
	protected static final String SPOUT_DELAY_RUN = "spout.delay.run";

	public static void setSpoutDelayRunSeconds(Map conf, int delay) {
		conf.put(SPOUT_DELAY_RUN, Integer.valueOf(delay));
	}

	public static int getSpoutDelayRunSeconds(Map conf) {
		return JStormUtils.parseInt(conf.get(SPOUT_DELAY_RUN), 30);
	}

	/**
	 * Default ZMQ Pending queue size
	 */
	public static final int DEFAULT_ZMQ_MAX_QUEUE_MSG = 1000;

	/**
	 * One task will alloc how many memory slot, the default setting is 1
	 */
	protected static final String MEM_SLOTS_PER_TASK = "memory.slots.per.task";

	public static void setMemSlotPerTask(Map conf, int slotNum) {
		if (slotNum < 1) {
			throw new InvalidParameterException();
		}
		conf.put(MEM_SLOTS_PER_TASK, Integer.valueOf(slotNum));
	}

	public static int getMemSlotPerTask(Map conf) {
		return JStormUtils.parseInt(conf.get(MEM_SLOTS_PER_TASK), 1);
	}

	/**
	 * One memory slot size, the unit size of memory, the default setting is 1G
	 * For example , if it is 1G, and one task set "memory.slots.per.task" as 2
	 * then the task will use 2G memory
	 */
	protected static final String MEM_SLOT_PER_SIZE = "memory.slot.per.size";

	public static long getMemSlotSize(Map conf) {
		return JStormUtils.parseLong(conf.get(MEM_SLOT_PER_SIZE),
				JStormUtils.SIZE_1_G);
	}

	/**
	 * One task will use cpu slot number, the default setting is 1
	 */
	protected static final String CPU_SLOTS_PER_TASK = "cpu.slots.per.task";

	public static void setCpuSlotsPerTask(Map conf, int slotNum) {
		if (slotNum < 1) {
			throw new InvalidParameterException();
		}
		conf.put(CPU_SLOTS_PER_TASK, Integer.valueOf(slotNum));
	}

	public static int getCpuSlotsPerTask(Map conf) {
		return JStormUtils.parseInt(conf.get(CPU_SLOTS_PER_TASK), 1);
	}

	/**
	 * One task alloc whether disk slot or not
	 */
	protected static final String TASK_ALLOC_DISK_SLOT = "task.alloc.disk.slot";

	public static void setTaskAllocDisk(Map conf, boolean alloc) {
		conf.put(TASK_ALLOC_DISK_SLOT, Boolean.valueOf(alloc));
	}

	public static boolean isTaskAllocDisk(Map conf) {
		return JStormUtils.parseBoolean(conf.get(TASK_ALLOC_DISK_SLOT), false);
	}

	/**
	 * The disk slot assigned to the task This configuration will be set by
	 * worker, application can't set it
	 */
	protected static final String TASK_ASSIGN_DISK_SLOT = "task.assign.disk.slot";

	public static String getTaskAssignDiskSlot(Map conf) {
		return (String) conf.get(TASK_ASSIGN_DISK_SLOT);
	}

	/**
	 * if the setting has been set, the component's task must run different node
	 * This is conflict with USE_SINGLE_NODE
	 */
	protected static final String TASK_ON_DIFFERENT_NODE = "task.on.differ.node";

	public static void setTaskOnDifferentNode(Map conf, boolean isIsolate) {
		conf.put(TASK_ON_DIFFERENT_NODE, Boolean.valueOf(isIsolate));
	}

	public static boolean isTaskOnDifferentNode(Map conf) {
		return JStormUtils
				.parseBoolean(conf.get(TASK_ON_DIFFERENT_NODE), false);
	}

	/**
	 * If component configuration set "use.userdefine.assignment", will try use
	 * user-defined assignment firstly
	 * 
	 * The task assigning algorith is as following
	 * 
	 * 1. if set USE_USERDEFINE_ASSIGNMENT, try use user-define assignment, if
	 * the user-define assignment is available, use it 2. if set
	 * USE_OLD_ASSIGNMENT, try use old assignment if the old assignment is
	 * available , use it 3. assign the task from free-pool
	 */
	protected static final String USE_USERDEFINE_ASSIGNMENT = "use.userdefine.assignment";

	public static void setUserDefineAssignment(Map conf,
			List<ResourceAssignment> userDefines) {
		conf.put(USE_USERDEFINE_ASSIGNMENT, userDefines);
	}

	/**
	 * After submit topology to JStorm, getUserDefineAssignmentFromJson should
	 * be used Before submit topology, getUserDefineAssignment should be used
	 * 
	 * @param conf
	 * @return
	 */
	public static List<ResourceAssignment> getUserDefineAssignment(Map conf) {
		return (List<ResourceAssignment>) conf.get(USE_USERDEFINE_ASSIGNMENT);
	}

	public static List<Object> getUserDefineAssignmentFromJson(Map conf) {
		return (List<Object>) conf.get(USE_USERDEFINE_ASSIGNMENT);
	}

	/**
	 * If component or topology configuration set "use.old.assignment", will try
	 * use old assignment firstly
	 */
	protected static final String USE_OLD_ASSIGNMENT = "use.old.assignment";

	public static void setUseOldAssignment(Map conf, boolean useOld) {
		conf.put(USE_OLD_ASSIGNMENT, Boolean.valueOf(useOld));
	}

	public static boolean isUseOldAssignment(Map conf) {
		return JStormUtils.parseBoolean(conf.get(USE_OLD_ASSIGNMENT), false);
	}

	/**
	 * Force all tasks have been assigned in one node, which is to reduce
	 * network consumer This is for samll topology This is conflict with
	 * TASK_ON_DIFFERENT_NODE
	 */
	protected static final String USE_SINGLE_NODE = "use.single.node.assignment";

	public static void setUseSingleNode(Map conf, boolean useSingle) {
		conf.put(USE_SINGLE_NODE, Boolean.valueOf(useSingle));
	}

	public static boolean isUseSingleNode(Map conf) {
		return JStormUtils.parseBoolean(conf.get(USE_SINGLE_NODE), false);
	}

	/**
	 * These priority weight will influence the topology assign which supervisor
	 * The core task assign algorithm like this supervisor.left.disk *
	 * TOPOLOGY_DISK_WEIGHT + supervisor.left.cpu * TOPOLOGY_CPU_WEIGHT +
	 * supervisor.left.mem * TOPOLOGY_MEM_WEIGHT + supervisor.left.port *
	 * TOPOLOGY_PORT_WEIGHT
	 * 
	 * the task will be assigned to the highest supervisor
	 * 
	 * by default TOPOLOGY_DISK_WEIGHT is 3 TOPOLOGY_CPU_WEIGHT is 1
	 * TOPOLOGY_MEM_WEIGHT is 1 TOPOLOGY_PORT_WEIGHT is 1
	 */
	protected static final String TOPOLOGY_DISK_WEIGHT = "topology.disk.weight";
	protected static final String TOPOLOGY_CPU_WEIGHT = "topology.cpu.weight";
	protected static final String TOPOLOGY_MEM_WEIGHT = "topology.memory.weight";
	protected static final String TOPOLOGY_PORT_WEIGHT = "topology.port.weight";

	public static final int DEFAULT_DISK_WEIGHT = 5;
	public static final int DEFAULT_CPU_WEIGHT = 3;
	public static final int DEFAULT_MEM_WEIGHT = 1;
	public static final int DEFAULT_PORT_WEIGHT = 2;

	public static void setTopologyDiskWeight(Map conf, int weight) {
		conf.put(TOPOLOGY_DISK_WEIGHT, Integer.valueOf(weight));
	}

	public static int getTopologyDiskWeight(Map conf) {
		return JStormUtils.parseInt(conf.get(TOPOLOGY_DISK_WEIGHT),
				DEFAULT_DISK_WEIGHT);
	}

	public static void setTopologyCpuWeight(Map conf, int weight) {
		conf.put(TOPOLOGY_CPU_WEIGHT, Integer.valueOf(weight));
	}

	public static int getTopologyCpuWeight(Map conf) {
		return JStormUtils.parseInt(conf.get(TOPOLOGY_CPU_WEIGHT),
				DEFAULT_CPU_WEIGHT);
	}

	public static void setTopologyMemWeight(Map conf, int weight) {
		conf.put(TOPOLOGY_MEM_WEIGHT, Integer.valueOf(weight));
	}

	public static int getTopologyMemWeight(Map conf) {
		return JStormUtils.parseInt(conf.get(TOPOLOGY_MEM_WEIGHT),
				DEFAULT_MEM_WEIGHT);
	}

	public static void setTopologyPortWeight(Map conf, int weight) {
		conf.put(TOPOLOGY_PORT_WEIGHT, Integer.valueOf(weight));
	}

	public static int getTopologyPortWeight(Map conf) {
		return JStormUtils.parseInt(conf.get(TOPOLOGY_PORT_WEIGHT),
				DEFAULT_PORT_WEIGHT);
	}

	protected static final String TOPOLOGY_ASSIGN_SUPERVISOR_BYLEVEL = "topology.assign.supervisor.bylevel";

	public static boolean isTopologyAssignSupervisorBylevel(Map conf) {
		return JStormUtils.parseBoolean(
				conf.get(TOPOLOGY_ASSIGN_SUPERVISOR_BYLEVEL), true);
	}

	public static void setTopologyAssignSupervisorBylevel(Map conf,
			boolean enable) {
		conf.put(TOPOLOGY_ASSIGN_SUPERVISOR_BYLEVEL, Boolean.valueOf(enable));
	}

	/**
	 * The supervisor's hostname
	 */
	protected static final String SUPERVISOR_HOSTNAME = "supervisor.hostname";
	public static final Object SUPERVISOR_HOSTNAME_SCHEMA = String.class;

	public static String getSupervisorHost(Map conf) {
		return (String) conf.get(SUPERVISOR_HOSTNAME);
	}

	protected static final String TOPOLOGY_ENABLE_CLASSLOADER = "topology.enable.classloader";

	public static boolean isEnableTopologyClassLoader(Map conf) {
		return JStormUtils.parseBoolean(conf.get(TOPOLOGY_ENABLE_CLASSLOADER),
				false);
	}

	public static void setEnableTopologyClassLoader(Map conf, boolean enable) {
		conf.put(TOPOLOGY_ENABLE_CLASSLOADER, Boolean.valueOf(enable));
	}

	protected static final String USER_GROUP = "user.group";

	public static void setUserGroup(Map conf, String groupName) {
		conf.put(USER_GROUP, groupName);
	}

	public static String getUserGroup(Map conf) {
		return (String) conf.get(USER_GROUP);
	}

	protected static final String USER_NAME = "user.name";

	public static void setUserName(Map conf, String userName) {
		conf.put(USER_NAME, userName);
	}

	public static String getUserName(Map conf) {
		return (String) conf.get(USER_NAME);
	}

	protected static final String USER_PASSWORD = "user.password";

	public static void setUserPassword(Map conf, String userPassword) {
		conf.put(USER_PASSWORD, userPassword);
	}

	public static String getUserPassword(Map conf) {
		return (String) conf.get(USER_PASSWORD);
	}

	protected static final String NIMBUS_GROUPFILE_PATH = "nimbus.groupfile.path";

	public static void setGroupFilePath(Map conf, String path) {
		conf.put(NIMBUS_GROUPFILE_PATH, path);
	}

	public static String getGroupFilePath(Map conf) {
		return (String) conf.get(NIMBUS_GROUPFILE_PATH);
	}

}