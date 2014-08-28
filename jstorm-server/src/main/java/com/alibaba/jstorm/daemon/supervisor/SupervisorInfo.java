package com.alibaba.jstorm.daemon.supervisor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Object stored in ZK /ZK-DIR/supervisors
 * 
 * @author Xin.Zhou/Longda
 */
public class SupervisorInfo implements Serializable {

	private static final long serialVersionUID = -8384417078907518922L;

	private final String hostName;
	private final String supervisorId;

	private Integer timeSecs;
	private Integer uptimeSecs;

	private Set<Integer> workerPorts;

	public SupervisorInfo(String hostName, String supervisorId,
			Set<Integer> workerPorts) {
		this.hostName = hostName;
		this.supervisorId = supervisorId;
		this.workerPorts = workerPorts;
	}

	public String getHostName() {
		return hostName;
	}

	public String getSupervisorId() {
		return supervisorId;
	}

	public int getTimeSecs() {
		return timeSecs;
	}

	public void setTimeSecs(int timeSecs) {
		this.timeSecs = timeSecs;
	}

	public int getUptimeSecs() {
		return uptimeSecs;
	}

	public void setUptimeSecs(int uptimeSecs) {
		this.uptimeSecs = uptimeSecs;
	}

	public Set<Integer> getWorkerPorts() {
		return workerPorts;
	}

	public void setWorkerPorts(Set<Integer> workerPorts) {
		this.workerPorts = workerPorts;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((hostName == null) ? 0 : hostName.hashCode());
		result = prime * result
				+ ((supervisorId == null) ? 0 : supervisorId.hashCode());
		result = prime * result
				+ ((timeSecs == null) ? 0 : timeSecs.hashCode());
		result = prime * result
				+ ((uptimeSecs == null) ? 0 : uptimeSecs.hashCode());
		result = prime * result
				+ ((workerPorts == null) ? 0 : workerPorts.hashCode());
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
		SupervisorInfo other = (SupervisorInfo) obj;
		if (hostName == null) {
			if (other.hostName != null)
				return false;
		} else if (!hostName.equals(other.hostName))
			return false;
		if (supervisorId == null) {
			if (other.supervisorId != null)
				return false;
		} else if (!supervisorId.equals(other.supervisorId))
			return false;
		if (timeSecs == null) {
			if (other.timeSecs != null)
				return false;
		} else if (!timeSecs.equals(other.timeSecs))
			return false;
		if (uptimeSecs == null) {
			if (other.uptimeSecs != null)
				return false;
		} else if (!uptimeSecs.equals(other.uptimeSecs))
			return false;
		if (workerPorts == null) {
			if (other.workerPorts != null)
				return false;
		} else if (!workerPorts.equals(other.workerPorts))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

	/**
	 * get Map<supervisorId, hostname>
	 * 
	 * @param stormClusterState
	 * @param callback
	 * @return
	 */
	public static Map<String, String> getNodeHost(
			Map<String, SupervisorInfo> supInfos) {

		Map<String, String> rtn = new HashMap<String, String>();

		for (Entry<String, SupervisorInfo> entry : supInfos.entrySet()) {

			SupervisorInfo superinfo = entry.getValue();

			String supervisorid = entry.getKey();

			rtn.put(supervisorid, superinfo.getHostName());

		}

		return rtn;
	}

}
