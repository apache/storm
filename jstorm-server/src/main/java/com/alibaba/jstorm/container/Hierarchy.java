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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dir == null) ? 0 : dir.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Hierarchy other = (Hierarchy) obj;
		if (dir == null) {
			if (other.dir != null)
				return false;
		} else if (!dir.equals(other.dir))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
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
