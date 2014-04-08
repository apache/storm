package com.alibaba.jstorm.container;

public class SubSystem {

	private SubSystemType type;

	private int hierarchyID;

	private int cgroupsNum;

	private boolean enable;

	public SubSystem(SubSystemType type, int hierarchyID, int cgroupNum,
			boolean enable) {
		this.type = type;
		this.hierarchyID = hierarchyID;
		this.cgroupsNum = cgroupNum;
		this.enable = enable;
	}

	public SubSystemType getType() {
		return type;
	}

	public void setType(SubSystemType type) {
		this.type = type;
	}

	public int getHierarchyID() {
		return hierarchyID;
	}

	public void setHierarchyID(int hierarchyID) {
		this.hierarchyID = hierarchyID;
	}

	public int getCgroupsNum() {
		return cgroupsNum;
	}

	public void setCgroupsNum(int cgroupsNum) {
		this.cgroupsNum = cgroupsNum;
	}

	public boolean isEnable() {
		return enable;
	}

	public void setEnable(boolean enable) {
		this.enable = enable;
	}

}
