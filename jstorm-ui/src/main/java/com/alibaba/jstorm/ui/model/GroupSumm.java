package com.alibaba.jstorm.ui.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.generated.ThriftResourceType;

public class GroupSumm implements Serializable {

	private static final long serialVersionUID = -317224517820149927L;
	private String name;
	private String totalCpu;
	private String usedCpu;
	private String totalMem;
	private String usedMem;
	private String totalDisk;
	private String usedDisk;
	private String totalPort;
	private String usedPort;

	public GroupSumm(String name, Map<ThriftResourceType, Integer> total,
			Map<ThriftResourceType, Integer> used) {
		this.name = name;
		for (Entry<ThriftResourceType, Integer> entry : total.entrySet()) {
			if (entry.getKey() == ThriftResourceType.CPU)
				totalCpu = getString(entry.getValue());
			else if (entry.getKey() == ThriftResourceType.MEM)
				totalMem = getString(entry.getValue());
			else if (entry.getKey() == ThriftResourceType.DISK)
				totalDisk = getString(entry.getValue());
			else if (entry.getKey() == ThriftResourceType.NET)
				totalPort = getString(entry.getValue());
		}
		for (Entry<ThriftResourceType, Integer> entry : used.entrySet()) {
			if (entry.getKey() == ThriftResourceType.CPU)
				usedCpu = getString(entry.getValue());
			else if (entry.getKey() == ThriftResourceType.MEM)
				usedMem = getString(entry.getValue());
			else if (entry.getKey() == ThriftResourceType.DISK)
				usedDisk = getString(entry.getValue());
			else if (entry.getKey() == ThriftResourceType.NET)
				usedPort = getString(entry.getValue());
		}
	}
	
	private String getString(Integer a) {
		return (a.intValue() == Integer.MAX_VALUE) ? "N/A" : String.valueOf(a);
	}

	public String getTotalCpu() {
		return totalCpu;
	}

	public void setTotalCpu(String totalCpu) {
		this.totalCpu = totalCpu;
	}

	public String getUsedCpu() {
		return usedCpu;
	}

	public void setUsedCpu(String usedCpu) {
		this.usedCpu = usedCpu;
	}

	public String getUsedDisk() {
		return usedDisk;
	}

	public void setUsedDisk(String usedDisk) {
		this.usedDisk = usedDisk;
	}

	public String getUsedPort() {
		return usedPort;
	}

	public void setUsedPort(String usedPort) {
		this.usedPort = usedPort;
	}

	public String getTotalPort() {
		return totalPort;
	}

	public void setTotalPort(String totalPort) {
		this.totalPort = totalPort;
	}

	public String getTotalDisk() {
		return totalDisk;
	}

	public void setTotalDisk(String totalDisk) {
		this.totalDisk = totalDisk;
	}

	public String getUsedMem() {
		return usedMem;
	}

	public void setUsedMem(String usedMem) {
		this.usedMem = usedMem;
	}

	public String getTotalMem() {
		return totalMem;
	}

	public void setTotalMem(String totalMem) {
		this.totalMem = totalMem;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
