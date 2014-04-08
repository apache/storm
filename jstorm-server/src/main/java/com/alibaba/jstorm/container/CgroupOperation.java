package com.alibaba.jstorm.container;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.alibaba.jstorm.container.cgroup.CgroupCommon;

public interface CgroupOperation {

	public List<Hierarchy> getHierarchies();

	public Set<SubSystem> getSubSystems();

	public boolean enabled(SubSystemType subsystem);

	public Hierarchy busy(SubSystemType subsystem);

	public Hierarchy mounted(Hierarchy hierarchy);

	public void mount(Hierarchy hierarchy) throws IOException;

	public void umount(Hierarchy hierarchy) throws IOException;

	public void create(CgroupCommon cgroup) throws SecurityException;

	public void delete(CgroupCommon cgroup) throws IOException;

}
