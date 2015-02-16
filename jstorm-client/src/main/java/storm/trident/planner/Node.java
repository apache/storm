package storm.trident.planner;

import backtype.storm.tuple.Fields;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class Node implements Serializable {
	private static AtomicInteger INDEX = new AtomicInteger(0);

	private String nodeId;

	public String name = null;
	public Fields allOutputFields;
	public String streamId;
	public Integer parallelismHint = null;
	public NodeStateInfo stateInfo = null;
	public int creationIndex;

	public Node(String streamId, String name, Fields allOutputFields) {
		this.nodeId = UUID.randomUUID().toString();
		this.allOutputFields = allOutputFields;
		this.streamId = streamId;
		this.name = name;
		this.creationIndex = INDEX.incrementAndGet();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
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
		Node other = (Node) obj;
		if (nodeId == null) {
			if (other.nodeId != null)
				return false;
		} else if (!nodeId.equals(other.nodeId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
	}

}
