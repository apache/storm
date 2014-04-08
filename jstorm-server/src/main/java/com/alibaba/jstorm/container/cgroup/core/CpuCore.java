package com.alibaba.jstorm.container.cgroup.core;

import java.io.IOException;
import java.util.List;

import com.alibaba.jstorm.container.CgroupUtils;
import com.alibaba.jstorm.container.Constants;
import com.alibaba.jstorm.container.SubSystemType;

public class CpuCore implements CgroupCore {

	public static final String CPU_SHARES = "/cpu.shares";
	public static final String CPU_RT_RUNTIME_US = "/cpu.rt_runtime_us";
	public static final String CPU_RT_PERIOD_US = "/cpu.rt_period_us";
	public static final String CPU_CFS_PERIOD_US = "/cpu.cfs_period_us";
	public static final String CPU_CFS_QUOTA_US = "/cpu.cfs_quota_us";
	public static final String CPU_STAT = "/cpu.stat";

	private final String dir;

	public CpuCore(String dir) {
		this.dir = dir;
	}

	@Override
	public SubSystemType getType() {
		// TODO Auto-generated method stub
		return SubSystemType.cpu;
	}

	public void setCpuShares(int weight) throws IOException {
		CgroupUtils.writeFileByLine(Constants.getDir(this.dir, CPU_SHARES),
				String.valueOf(weight));
	}

	public int getCpuShares() throws IOException {
		return Integer.parseInt(CgroupUtils.readFileByLine(
				Constants.getDir(this.dir, CPU_SHARES)).get(0));
	}

	public void setCpuRtRuntimeUs(long us) throws IOException {
		CgroupUtils.writeFileByLine(
				Constants.getDir(this.dir, CPU_RT_RUNTIME_US),
				String.valueOf(us));
	}

	public long getCpuRtRuntimeUs() throws IOException {
		return Long.parseLong(CgroupUtils.readFileByLine(
				Constants.getDir(this.dir, CPU_RT_RUNTIME_US)).get(0));
	}

	public void setCpuRtPeriodUs(long us) throws IOException {
		CgroupUtils.writeFileByLine(
				Constants.getDir(this.dir, CPU_RT_PERIOD_US),
				String.valueOf(us));
	}

	public Long getCpuRtPeriodUs() throws IOException {
		return Long.parseLong(CgroupUtils.readFileByLine(
				Constants.getDir(this.dir, CPU_RT_PERIOD_US)).get(0));
	}

	public void setCpuCfsPeriodUs(long us) throws IOException {
		CgroupUtils.writeFileByLine(
				Constants.getDir(this.dir, CPU_CFS_PERIOD_US),
				String.valueOf(us));
	}

	public Long getCpuCfsPeriodUs(long us) throws IOException {
		return Long.parseLong(CgroupUtils.readFileByLine(
				Constants.getDir(this.dir, CPU_CFS_PERIOD_US)).get(0));
	}

	public void setCpuCfsQuotaUs(long us) throws IOException {
		CgroupUtils.writeFileByLine(
				Constants.getDir(this.dir, CPU_CFS_QUOTA_US),
				String.valueOf(us));
	}

	public Long getCpuCfsQuotaUs(long us) throws IOException {
		return Long.parseLong(CgroupUtils.readFileByLine(
				Constants.getDir(this.dir, CPU_CFS_QUOTA_US)).get(0));
	}

	public Stat getCpuStat() throws IOException {
		return new Stat(CgroupUtils.readFileByLine(Constants.getDir(this.dir,
				CPU_STAT)));
	}

	public static class Stat {
		public final int nrPeriods;
		public final int nrThrottled;
		public final int throttledTime;

		public Stat(List<String> statStr) {
			this.nrPeriods = Integer.parseInt(statStr.get(0).split(" ")[1]);
			this.nrThrottled = Integer.parseInt(statStr.get(1).split(" ")[1]);
			this.throttledTime = Integer.parseInt(statStr.get(2).split(" ")[1]);
		}

		@Override
		public boolean equals(Object obj) {
			Stat excepted = (Stat) obj;
			boolean result = false;
			if (this.nrPeriods == excepted.nrPeriods
					&& this.nrThrottled == excepted.nrThrottled
					&& this.throttledTime == excepted.throttledTime) {
				result = true;
			}
			return result;
		}
	}

}
