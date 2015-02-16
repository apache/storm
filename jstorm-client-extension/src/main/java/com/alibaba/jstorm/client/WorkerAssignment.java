package com.alibaba.jstorm.client;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;
import org.json.simple.JSONAware;

import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.Utils;


public class WorkerAssignment extends WorkerSlot implements Serializable,
		JSONAware {
	private static final Logger LOG = Logger.getLogger(WorkerAssignment.class);


	private static final long serialVersionUID = -3483047434535537861L;

	private Map<String, Integer> componentToNum = new HashMap<String, Integer>();

	private long mem;

	private int cpu;

	private String hostName;

	private String jvm;
	
	private static final String COMPONENTTONUM_TAG = "componentToNum";
	private static final String MEM_TAG = "mem";
	private static final String CPU_TAG = "cpu";
	private static final String HOSTNAME_TAG = "hostName";
	private static final String JVM_TAG = "jvm";
	private static final String NODEID_TAG = "nodeId";
	private static final String PORT_TAG = "port";

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
//		StringBuilder sb = new StringBuilder();

//		sb.append("[");
//		sb.append("\"" + this.getNodeId() + "\"");
//		sb.append(",");
//		sb.append("\"" + this.hostName + "\"");
//		sb.append(",");
//		sb.append("\"" + String.valueOf(this.getPort()) + "\"");
//		sb.append(",");
//		sb.append("\"" + this.jvm + "\"");
//		sb.append(",");
//		sb.append("\"" + String.valueOf(this.mem) + "\"");
//		sb.append(",");
//		sb.append("\"" + String.valueOf(this.cpu) + "\"");
//		sb.append(",");
//		sb.append("{");
//		for (Entry<String, Integer> entry : componentToNum.entrySet()) {
//			sb.append("\"" + entry.getKey() + "\":");
//			sb.append("\"" + String.valueOf(entry.getValue()) + "\"");
//			sb.append(",");
//		}
//		sb.append("}");
//		sb.append("]");
		
		
		
		Map<String, String> map = new HashMap<String, String>();

		map.put(COMPONENTTONUM_TAG, Utils.to_json(componentToNum));
		map.put(MEM_TAG, String.valueOf(mem));
		map.put(CPU_TAG, String.valueOf(cpu));
		map.put(HOSTNAME_TAG, hostName);
		map.put(JVM_TAG, jvm);
		map.put(NODEID_TAG, getNodeId());
		map.put(PORT_TAG, String.valueOf(getPort()));
	
		
		return Utils.to_json(map);
	}

	public static WorkerAssignment parseFromObj(Object obj) {
		if (obj == null) {
			return null;
		}

		if (obj instanceof Map == false) {
			return null;
		}

		try {
			Map<String, String> map = (Map<String, String>)obj;
			
			String supervisorId = map.get(NODEID_TAG);
			String hostname = map.get(HOSTNAME_TAG);
			Integer port = JStormUtils.parseInt(map.get(PORT_TAG));
			String jvm = map.get(JVM_TAG);
			Long mem = JStormUtils.parseLong(map.get(MEM_TAG));
			Integer cpu = JStormUtils.parseInt(map.get(CPU_TAG));
			Map<String, Object> componentToNum = (Map<String, Object>)Utils.from_json(map.get(COMPONENTTONUM_TAG));

			WorkerAssignment ret = new WorkerAssignment(supervisorId, port);

			
			ret.hostName = hostname;
			ret.setNodeId(supervisorId);
			ret.setJvm(jvm);
			if (port != null) {
				ret.setPort(port);
			}
			if (mem != null) {
				ret.setMem(mem);
			}
			if (cpu != null) {
				ret.setCpu(cpu);
			}
			
			for (Entry<String, Object> entry : componentToNum.entrySet()) {
				ret.addComponent(entry.getKey(),
						JStormUtils.parseInt(entry.getValue()));
			}
			return ret;
		} catch (Exception e) {
			LOG.error("Failed to convert to WorkerAssignment,  raw:" + obj, e);
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

		 input.setJvm("sb");
		
		 input.setCpu(1);
		
		 input.setMem(2);

		 input.addComponent("2b", 2);

		String outString = Utils.to_json(input);

		System.out.println(input);
		
		//String outString = "[componentToNum={},mem=1610612736,cpu=1,hostName=mobilejstorm-60-1,jvm=<null>,nodeId=<null>,port=0]";

		Object object = Utils.from_json(outString);
		System.out.println(object);

		System.out.println(parseFromObj(object));

		System.out.print(input.equals(parseFromObj(object)));
	}
	

}
