package backtype.storm.scheduler;

import java.io.Serializable;

public class WorkerSlot implements Comparable<WorkerSlot>, Serializable {

	private static final long serialVersionUID = -4451854497340313268L;
	String nodeId;
	int port;

	public WorkerSlot(String nodeId, Number port) {
		this.nodeId = nodeId;
		this.port = port.intValue();
	}
	
	public WorkerSlot() {
		
	}

	public String getNodeId() {
		return nodeId;
	}

	public int getPort() {
		return port;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
		result = prime * result + port;
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
		WorkerSlot other = (WorkerSlot) obj;
		if (nodeId == null) {
			if (other.nodeId != null)
				return false;
		} else if (!nodeId.equals(other.nodeId))
			return false;
		if (port != other.port)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return this.nodeId + ":" + this.port;
	}

	@Override
	public int compareTo(WorkerSlot o) {
		String otherNode = o.getNodeId();
		if (nodeId == null) {
			if (otherNode != null) {
				return -1;
			} else {
				return port - o.getPort();
			}
		} else {
			int ret = nodeId.compareTo(otherNode);
			if (ret == 0) {
				return port - o.getPort();
			} else {
				return ret;
			}
		}
	}
}
