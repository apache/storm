package com.alibaba.jstorm.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.client.WorkerAssignment;

public class JStromServerConfigExtension extends ConfigExtension {

	public static List<WorkerAssignment> getUserDefineAssignment(Map conf) {
		List<WorkerAssignment> ret = new ArrayList<WorkerAssignment>();
		if (conf.get(USE_USERDEFINE_ASSIGNMENT) == null)
			return ret;
		for (String worker : (List<String>) conf.get(USE_USERDEFINE_ASSIGNMENT)) {
			ret.add(WorkerAssignment.parseFromObj(JSON.parse(worker)));
		}
		return ret;
	}

	public static boolean isUseOldAssignment(Map conf) {
		return JStormUtils.parseBoolean(conf.get(USE_OLD_ASSIGNMENT), false);
	}

	public static long getMemSizePerWorker(Map conf) {
		long size = JStormUtils.parseLong(conf.get(MEMSIZE_PER_WORKER),
				JStormUtils.SIZE_1_G * 2);
		return size > 0 ? size : JStormUtils.SIZE_1_G * 2;
	}

	public static int getCpuSlotPerWorker(Map conf) {
		int slot = JStormUtils.parseInt(conf.get(CPU_SLOT_PER_WORKER), 1);
		return slot > 0 ? slot : 1;
	}
}
