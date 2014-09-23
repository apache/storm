package com.alibaba.jstorm.metric;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import com.codahale.metrics.Gauge;

public class MetricJstack implements Gauge<String> {

	private String getTaskName(long id, String name) {
		if (name == null) {
			return Long.toString(id);
		}
		return id + " (" + name + ")";
	}

	public String dumpThread() throws Exception {
		StringBuilder writer = new StringBuilder();

		ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

		boolean contention = threadMXBean.isThreadContentionMonitoringEnabled();

		long[] threadIds = threadMXBean.getAllThreadIds();
		writer.append(threadIds.length + " active threads:");
		for (long tid : threadIds) {
			writer.append(tid).append(" ");
		}
		writer.append("\n");

		long[] deadLockTids = threadMXBean.findDeadlockedThreads();
		if (deadLockTids != null) {
			writer.append(threadIds.length + " deadlocked threads:");
			for (long tid : deadLockTids) {
				writer.append(tid).append(" ");
			}
			writer.append("\n");
		}

		long[] deadLockMonitorTids = threadMXBean
				.findMonitorDeadlockedThreads();
		if (deadLockMonitorTids != null) {
			writer.append(threadIds.length + " deadlocked monitor threads:");
			for (long tid : deadLockMonitorTids) {
				writer.append(tid).append(" ");
			}
			writer.append("\n");
		}

		for (long tid : threadIds) {
			ThreadInfo info = threadMXBean
					.getThreadInfo(tid, Integer.MAX_VALUE);
			if (info == null) {
				writer.append("  Inactive").append("\n");
				continue;
			}
			writer.append(
					"Thread "
							+ getTaskName(info.getThreadId(),
									info.getThreadName()) + ":").append("\n");
			Thread.State state = info.getThreadState();
			writer.append("  State: " + state).append("\n");
			writer.append("  Blocked count: " + info.getBlockedCount()).append(
					"\n");
			writer.append("  Waited count: " + info.getWaitedCount()).append(
					"\n");
			writer.append(" Cpu time:")
					.append(threadMXBean.getThreadCpuTime(tid) / 1000000)
					.append("ms").append("\n");
			writer.append(" User time:")
					.append(threadMXBean.getThreadUserTime(tid) / 1000000)
					.append("ms").append("\n");
			if (contention) {
				writer.append("  Blocked time: " + info.getBlockedTime())
						.append("\n");
				writer.append("  Waited time: " + info.getWaitedTime()).append(
						"\n");
			}
			if (state == Thread.State.WAITING) {
				writer.append("  Waiting on " + info.getLockName())
						.append("\n");
			} else if (state == Thread.State.BLOCKED) {
				writer.append("  Blocked on " + info.getLockName())
						.append("\n");
				writer.append(
						"  Blocked by "
								+ getTaskName(info.getLockOwnerId(),
										info.getLockOwnerName())).append("\n");
			}

		}
		for (long tid : threadIds) {
			ThreadInfo info = threadMXBean
					.getThreadInfo(tid, Integer.MAX_VALUE);
			if (info == null) {
				writer.append("  Inactive").append("\n");
				continue;
			}

			writer.append(
					"Thread "
							+ getTaskName(info.getThreadId(),
									info.getThreadName()) + ": Stack").append(
					"\n");
			for (StackTraceElement frame : info.getStackTrace()) {
				writer.append("    " + frame.toString()).append("\n");
			}
		}

		return writer.toString();
	}

	@Override
	public String getValue() {
		try {
			return dumpThread();
		} catch (Exception e) {
			return "Failed to get jstack thread info";
		}
	}

}
