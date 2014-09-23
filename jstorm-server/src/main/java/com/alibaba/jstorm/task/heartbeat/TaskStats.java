package com.alibaba.jstorm.task.heartbeat;

import com.alibaba.jstorm.stats.CommonStatsRolling;

public class TaskStats {
		private CommonStatsRolling taskStats;
		private String componentType;
		
		public TaskStats(String componentType, CommonStatsRolling taskStats) {
			this.componentType = componentType;
			this.taskStats = taskStats;
		}
		
		public CommonStatsRolling getTaskStat() {
			return taskStats;
		}
		
		public String getComponentType() {
			return componentType;
		}
}