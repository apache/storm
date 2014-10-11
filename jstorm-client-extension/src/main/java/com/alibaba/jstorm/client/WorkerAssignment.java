package com.alibaba.jstorm.client;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;

import backtype.storm.scheduler.WorkerSlot;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;

public class WorkerAssignment extends WorkerSlot implements Serializable,
		JSONAware {

	public static Logger LOG = Logger.getLogger(WorkerAssignment.class);

	private static final long serialVersionUID = -3483047434535537861L;

	private Map<String, Integer> componentToNum = new HashMap<String, Integer>();

	private long mem;

	private int cpu;

	private String hostName;

	private String jvm;

	public WorkerAssignment(String nodeId, Number port) {
		super(nodeId, port);
		// TODO Auto-generated constructor stub
	}

	public WorkerAssignment() {

	}

	public void addComponent(String compenentName, Integer num) {
		componentToNum.put(compenentName, num);
	}

	public Map<String, Integer> getComponentToNum() {
		return componentToNum;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public void setJvm(String jvm) {
		this.jvm = jvm;
	}
	
	public String getJvm() {
		return jvm;
	}

	public long getMem() {
		return mem;
	}

	public void setMem(long mem) {
		this.mem = mem;
	}

	public int getCpu() {
		return cpu;
	}

	public void setCpu(int cpu) {
		this.cpu = cpu;
	}

	@Override
	public String toJSONString() {
		StringBuilder sb = new StringBuilder();

		sb.append("[");
		sb.append("\"" + this.getNodeId() + "\"");
		sb.append(",");
		sb.append("\"" + this.hostName + "\"");
		sb.append(",");
		sb.append("\"" + String.valueOf(this.getPort()) + "\"");
		sb.append(",");
		sb.append("\"" + this.jvm + "\"");
		sb.append(",");
		sb.append("\"" + String.valueOf(this.mem) + "\"");
		sb.append(",");
		sb.append("\"" + String.valueOf(this.cpu) + "\"");
		sb.append(",");
		sb.append("{");
		for (Entry<String, Integer> entry : componentToNum.entrySet()) {
			sb.append("\"" + entry.getKey() + "\":");
			sb.append("\"" + String.valueOf(entry.getValue()) + "\"");
			sb.append(",");
		}
		sb.append("}");
		sb.append("]");

		return sb.toString();
	}

	public static WorkerAssignment parseFromObj(Object obj) {
		if (obj == null) {
			return null;
		}

		if (obj instanceof List<?> == false) {
			return null;
		}

		try {
			List list = (List) obj;
			if (list.size() < 6) {
				return null;
			}
			String supervisorId = getStringFromJson((String) list.get(0));
			String hostname = getStringFromJson((String) list.get(1));
			int port = Integer.parseInt(((String) list.get(2)));
			String jvm = getStringFromJson((String) list.get(3));
			long mem = Long.parseLong((String) list.get(4));
			int cpu = Integer.parseInt(((String) list.get(5)));
			Map<String, String> componentToNum = (Map<String, String>) list
					.get(6);

			WorkerAssignment ret = new WorkerAssignment(supervisorId, port);

			ret.hostName = hostname;
			ret.setNodeId(supervisorId);
			ret.setPort(port);
			ret.setJvm(jvm);
			ret.setMem(mem);
			ret.setCpu(cpu);
			for (Entry<String, String> entry : componentToNum.entrySet()) {
				ret.addComponent(entry.getKey(),
						Integer.valueOf(entry.getValue()));
			}
			return ret;
		} catch (Exception e) {
			return null;
		}

	}

	public static String getStringFromJson(String text) {
		return text.equals("null") ? null : text;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((componentToNum == null) ? 0 : componentToNum.hashCode());
		result = prime * result + cpu;
		result = prime * result
				+ ((hostName == null) ? 0 : hostName.hashCode());
		result = prime * result + ((jvm == null) ? 0 : jvm.hashCode());
		result = prime * result + (int) (mem ^ (mem >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		WorkerAssignment other = (WorkerAssignment) obj;
		if (componentToNum == null) {
			if (other.componentToNum != null)
				return false;
		} else if (!componentToNum.equals(other.componentToNum))
			return false;
		if (cpu != other.cpu)
			return false;
		if (hostName == null) {
			if (other.hostName != null)
				return false;
		} else if (!hostName.equals(other.hostName))
			return false;
		if (jvm == null) {
			if (other.jvm != null)
				return false;
		} else if (!jvm.equals(other.jvm))
			return false;
		if (mem != other.mem)
			return false;
		return true;
	}

	public static void main(String[] args) {
		WorkerAssignment input = new WorkerAssignment();

		// input.setJvm("sb");
		//
		// input.setCpu(1);
		//
		// input.setMem(2);

		// input.addComponent("2b", 2);

		String outString = JSON.toJSONString(input);

		System.out.println(input);

		Object object = JSON.parse(outString);

		System.out.println(parseFromObj(object));

		System.out.print(input.equals(parseFromObj(object)));
	}

}
