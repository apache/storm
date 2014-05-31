package com.alibaba.jstorm.ui.model;

/**
 * mainpage:ClusterSummary
 * @author xin.zhou/zhiyuan.ls
 *
 */
import java.io.Serializable;

public class ClusterSumm implements Serializable {
	private static final long serialVersionUID = -7936384220562644886L;
	
	private String nimbusIp;
	private String nimbusLogPort;
	private String nimbusHostname;
	private String nimbusUptime;
	private String isGroupModel;
	private String supervisorNum;
	private String runningTaskNum;
	private String totalCpuSlotNum;
	private String usedCpuSlotNum;
	private String freeCpuSlotNum;
	private String totalMemSlotNum;
	private String usedMemSlotNum;
	private String freeMemSlotNum;
	private String totalDiskSlotNum;
	private String usedDiskSlotNum;
	private String freeDiskSlotNum;
	private String totalPortSlotNum;
	private String usedPortSlotNum;
	private String freePortSlotNum;

	public String getNimbusIp() {
		return nimbusIp;
	}

	public void setNimbusIp(String nimbusIp) {
		this.nimbusIp = nimbusIp;
	}
	
	public String getNimbusLogPort() {
		return nimbusLogPort;
	}

	public void setNimbusLogPort(String nimbusLogPort) {
		this.nimbusLogPort = nimbusLogPort;
	}

	public String getNimbusHostname() {
		return nimbusHostname;
	}

	public void setNimbusHostname(String nimbusHostname) {
		this.nimbusHostname = nimbusHostname;
	}

	public String getNimbusUptime() {
		return nimbusUptime;
	}

	public void setNimbusUptime(String nimbusUptime) {
		this.nimbusUptime = nimbusUptime;
	}

	public String getSupervisorNum() {
		return supervisorNum;
	}

	public void setSupervisorNum(String supervisorNum) {
		this.supervisorNum = supervisorNum;
	}

	public String getRunningTaskNum() {
		return runningTaskNum;
	}

	public void setRunningTaskNum(String runningTaskNum) {
		this.runningTaskNum = runningTaskNum;
	}

	public String getTotalCpuSlotNum() {
		return totalCpuSlotNum;
	}

	public void setTotalCpuSlotNum(String totalCpuSlotNum) {
		this.totalCpuSlotNum = totalCpuSlotNum;
	}

	public String getUsedCpuSlotNum() {
		return usedCpuSlotNum;
	}

	public void setUsedCpuSlotNum(String usedCpuSlotNum) {
		this.usedCpuSlotNum = usedCpuSlotNum;
	}

	public String getFreeCpuSlotNum() {
		return freeCpuSlotNum;
	}

	public void setFreeCpuSlotNum(String freeCpuSlotNum) {
		this.freeCpuSlotNum = freeCpuSlotNum;
	}

	public String getTotalMemSlotNum() {
		return totalMemSlotNum;
	}

	public void setTotalMemSlotNum(String totalMemSlotNum) {
		this.totalMemSlotNum = totalMemSlotNum;
	}

	public String getUsedMemSlotNum() {
		return usedMemSlotNum;
	}

	public void setUsedMemSlotNum(String usedMemSlotNum) {
		this.usedMemSlotNum = usedMemSlotNum;
	}

	public String getFreeMemSlotNum() {
		return freeMemSlotNum;
	}

	public void setFreeMemSlotNum(String freeMemSlotNum) {
		this.freeMemSlotNum = freeMemSlotNum;
	}

	public String getTotalDiskSlotNum() {
		return totalDiskSlotNum;
	}

	public void setTotalDiskSlotNum(String totalDiskSlotNum) {
		this.totalDiskSlotNum = totalDiskSlotNum;
	}

	public String getUsedDiskSlotNum() {
		return usedDiskSlotNum;
	}

	public void setUsedDiskSlotNum(String usedDiskSlotNum) {
		this.usedDiskSlotNum = usedDiskSlotNum;
	}

	public String getFreeDiskSlotNum() {
		return freeDiskSlotNum;
	}

	public void setFreeDiskSlotNum(String freeDiskSlotNum) {
		this.freeDiskSlotNum = freeDiskSlotNum;
	}

	public String getTotalPortSlotNum() {
		return totalPortSlotNum;
	}

	public void setTotalPortSlotNum(String totalPortSlotNum) {
		this.totalPortSlotNum = totalPortSlotNum;
	}

	public String getUsedPortSlotNum() {
		return usedPortSlotNum;
	}

	public void setUsedPortSlotNum(String usedPortSlotNum) {
		this.usedPortSlotNum = usedPortSlotNum;
	}

	public String getFreePortSlotNum() {
		return freePortSlotNum;
	}

	public void setFreePortSlotNum(String freePortSlotNum) {
		this.freePortSlotNum = freePortSlotNum;
	}

	public String getIsGroupModel() {
		return isGroupModel;
	}

	public void setIsGroupModel(String isGroupModel) {
		this.isGroupModel = isGroupModel;
	}

}
