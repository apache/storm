package com.alibaba.jstorm.container;

import java.util.Set;
import com.alibaba.jstorm.container.cgroup.CgroupCommon;

public class Hierarchy {

	private final String name;

	private final Set<SubSystemType> subSystems;

	private final String type;

	private final String dir;

	private final CgroupCommon rootCgroups;

	public Hierarchy(String name, Set<SubSystemType> subSystems, String dir) {
		this.name = name;
		this.subSystems = subSystems;
		this.dir = dir;
		this.rootCgroups = new CgroupCommon(this, dir);
		this.type = CgroupUtils.reAnalyse(subSystems);
	}

	public Set<SubSystemType> getSubSystems() {
		return subSystems;
	}

	public String getType() {
		return type;
	}

	@Override
	public boolean equals(Object h) {
		if (h instanceof Hierarchy && ((Hierarchy) h).type.equals(this.type))
			return true;
		return false;
	}

	public String getDir() {
		return dir;
	}

	public CgroupCommon getRootCgroups() {
		return rootCgroups;
	}

	public String getName() {
		return name;
	}

	public boolean subSystemMounted(SubSystemType subsystem) {
		for (SubSystemType type : this.subSystems) {
			if (type == subsystem)
				return true;
		}
		return false;
	}

}
