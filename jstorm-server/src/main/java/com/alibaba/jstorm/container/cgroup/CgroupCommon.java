package com.alibaba.jstorm.container.cgroup;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.jstorm.container.CgroupUtils;
import com.alibaba.jstorm.container.Constants;
import com.alibaba.jstorm.container.Hierarchy;
import com.alibaba.jstorm.container.SubSystemType;
import com.alibaba.jstorm.container.cgroup.core.CgroupCore;

public class CgroupCommon implements CgroupCommonOperation {

	public static final String TASKS = "/tasks";
	public static final String NOTIFY_ON_RELEASE = "/notify_on_release";
	public static final String RELEASE_AGENT = "/release_agent";
	public static final String CGROUP_CLONE_CHILDREN = "/cgroup.clone_children";
	public static final String CGROUP_EVENT_CONTROL = "/cgroup.event_control";
	public static final String CGROUP_PROCS = "/cgroup.procs";

	private final Hierarchy hierarchy;

	private final String name;

	private final String dir;

	private final CgroupCommon parent;

	private final Map<SubSystemType, CgroupCore> cores;

	private final boolean isRoot;

	private final Set<CgroupCommon> children = new HashSet<CgroupCommon>();

	public CgroupCommon(String name, Hierarchy hierarchy, CgroupCommon parent) {
		this.name = parent.getName() + "/" + name;
		this.hierarchy = hierarchy;
		this.parent = parent;
		this.dir = parent.getDir() + "/" + name;
		this.init();
		cores = CgroupCoreFactory.getInstance(this.hierarchy.getSubSystems(),
				this.dir);
		this.isRoot = false;
	}

	/**
	 * rootCgroup
	 */
	public CgroupCommon(Hierarchy hierarchy, String dir) {
		this.name = "";
		this.hierarchy = hierarchy;
		this.parent = null;
		this.dir = dir;
		this.init();
		cores = CgroupCoreFactory.getInstance(this.hierarchy.getSubSystems(),
				this.dir);
		this.isRoot = true;
	}

	@Override
	public void addTask(int taskId) throws IOException {
		// TODO Auto-generated method stub
		CgroupUtils.writeFileByLine(Constants.getDir(this.dir, TASKS),
				String.valueOf(taskId));
	}

	@Override
	public Set<Integer> getTasks() throws IOException {
		List<String> stringTasks = CgroupUtils.readFileByLine(Constants.getDir(
				this.dir, TASKS));
		Set<Integer> tasks = new HashSet<Integer>();
		for (String task : stringTasks) {
			tasks.add(Integer.valueOf(task));
		}
		return tasks;
	}

	@Override
	public void addProcs(int pid) throws IOException {
		// TODO Auto-generated method stub
		CgroupUtils.writeFileByLine(Constants.getDir(this.dir, CGROUP_PROCS),
				String.valueOf(pid));
	}

	@Override
	public Set<Integer> getPids() throws IOException {
		// TODO Auto-generated method stub
		List<String> stringPids = CgroupUtils.readFileByLine(Constants.getDir(
				this.dir, CGROUP_PROCS));
		Set<Integer> pids = new HashSet<Integer>();
		for (String task : stringPids) {
			pids.add(Integer.valueOf(task));
		}
		return pids;
	}

	@Override
	public void setNotifyOnRelease(boolean flag) throws IOException {
		// TODO Auto-generated method stub
		CgroupUtils
				.writeFileByLine(Constants.getDir(this.dir, NOTIFY_ON_RELEASE),
						flag ? "1" : "0");
	}

	@Override
	public boolean getNotifyOnRelease() throws IOException {
		return CgroupUtils
				.readFileByLine(Constants.getDir(this.dir, NOTIFY_ON_RELEASE))
				.get(0).equals("1") ? true : false;
	}

	@Override
	public void setReleaseAgent(String command) throws IOException {
		// TODO Auto-generated method stub
		if (!this.isRoot)
			return;
		CgroupUtils.writeFileByLine(Constants.getDir(this.dir, RELEASE_AGENT),
				command);
	}

	@Override
	public String getReleaseAgent() throws IOException {
		if (!this.isRoot)
			return null;
		return CgroupUtils.readFileByLine(
				Constants.getDir(this.dir, RELEASE_AGENT)).get(0);
	}

	@Override
	public void setCgroupCloneChildren(boolean flag) throws IOException {
		// TODO Auto-generated method stub
		if (!this.cores.keySet().contains(SubSystemType.cpuset))
			return;
		CgroupUtils.writeFileByLine(Constants.getDir(this.dir,
				CGROUP_CLONE_CHILDREN), flag ? "1" : "0");
	}

	@Override
	public boolean getCgroupCloneChildren() throws IOException {
		return CgroupUtils
				.readFileByLine(
						Constants.getDir(this.dir, CGROUP_CLONE_CHILDREN))
				.get(0).equals("1") ? true : false;
	}

	@Override
	public void setEventControl(String eventFd, String controlFd,
			String... args) throws IOException {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		sb.append(eventFd);
		sb.append(' ');
		sb.append(controlFd);
		for (String arg : args) {
			sb.append(' ');
			sb.append(arg);
		}
		CgroupUtils
				.writeFileByLine(
						Constants.getDir(this.dir, CGROUP_EVENT_CONTROL),
						sb.toString());
	}

	public Hierarchy getHierarchy() {
		return hierarchy;
	}

	public String getName() {
		return name;
	}

	public String getDir() {
		return dir;
	}

	public CgroupCommon getParent() {
		return parent;
	}

	public Set<CgroupCommon> getChildren() {
		return children;
	}

	public boolean isRoot() {
		return isRoot;
	}

	public Map<SubSystemType, CgroupCore> getCores() {
		return cores;
	}

	public void delete() throws IOException {
		this.free();
		if (!this.isRoot)
			this.parent.getChildren().remove(this);
	}

	private void free() throws IOException {
		for (CgroupCommon child : this.children)
			child.free();
		if (this.isRoot)
			return;
		Set<Integer> tasks = this.getTasks();
		if (tasks != null) {
			for (Integer task : tasks) {
				this.parent.addTask(task);
			}
		}
		CgroupUtils.deleteDir(this.dir);
	}

	private void init() {
		File file = new File(this.dir);
		File[] files = file.listFiles();
		if (files == null)
			return;
		for (File child : files) {
			if (child.isDirectory()) {
				this.children.add(new CgroupCommon(child.getName(),
						this.hierarchy, this));
			}
		}
	}

}
