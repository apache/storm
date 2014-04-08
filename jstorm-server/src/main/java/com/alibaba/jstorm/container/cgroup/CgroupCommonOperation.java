package com.alibaba.jstorm.container.cgroup;

import java.io.IOException;
import java.util.Set;

public interface CgroupCommonOperation {

	public void addTask(int taskid) throws IOException;

	public Set<Integer> getTasks() throws IOException;

	public void addProcs(int pid) throws IOException;

	public Set<Integer> getPids() throws IOException;

	public void setNotifyOnRelease(boolean flag) throws IOException;

	public boolean getNotifyOnRelease() throws IOException;

	public void setReleaseAgent(String command) throws IOException;

	public String getReleaseAgent() throws IOException;

	public void setCgroupCloneChildren(boolean flag) throws IOException;

	public boolean getCgroupCloneChildren() throws IOException;

	public void setEventControl(String eventFd, String controlFd,
			String... args) throws IOException;

}
