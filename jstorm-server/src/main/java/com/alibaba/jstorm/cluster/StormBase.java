package com.alibaba.jstorm.cluster;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.jstorm.daemon.nimbus.StatusType;

/**
 * Topology stored in ZK
 */

public class StormBase implements Serializable {

	private static final long serialVersionUID = -3013095336395395213L;
	private String stormName;
	private int lanchTimeSecs;
	private StormStatus status;
	private String group;

	public StormBase(String stormName, int lanchTimeSecs, StormStatus status,
			String group) {
		this.stormName = stormName;
		this.lanchTimeSecs = lanchTimeSecs;
		this.status = status;
		this.setGroup(group);
	}

	public String getStormName() {
		return stormName;
	}

	public void setStormName(String stormName) {
		this.stormName = stormName;
	}

	public int getLanchTimeSecs() {
		return lanchTimeSecs;
	}

	public void setLanchTimeSecs(int lanchTimeSecs) {
		this.lanchTimeSecs = lanchTimeSecs;
	}

	public StormStatus getStatus() {
		return status;
	}

	public void setStatus(StormStatus status) {
		this.status = status;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((group == null) ? 0 : group.hashCode());
		result = prime * result + lanchTimeSecs;
		result = prime * result + ((status == null) ? 0 : status.hashCode());
		result = prime * result
				+ ((stormName == null) ? 0 : stormName.hashCode());
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
		StormBase other = (StormBase) obj;
		if (group == null) {
			if (other.group != null)
				return false;
		} else if (!group.equals(other.group))
			return false;
		if (lanchTimeSecs != other.lanchTimeSecs)
			return false;
		if (status == null) {
			if (other.status != null)
				return false;
		} else if (!status.equals(other.status))
			return false;
		if (stormName == null) {
			if (other.stormName != null)
				return false;
		} else if (!stormName.equals(other.stormName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

	public String getStatusString() {
		StatusType t = status.getStatusType();
		return t.getStatus().toUpperCase();
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

}
