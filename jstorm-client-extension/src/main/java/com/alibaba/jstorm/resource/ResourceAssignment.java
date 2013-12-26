package com.alibaba.jstorm.resource;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.json.simple.JSONAware;
import org.json.simple.JSONValue;

//one task 's assignment
public class ResourceAssignment implements Serializable, JSONAware {
    /**  */
    private static final long serialVersionUID = 9138386287559932411L;

    private String            supervisorId;
    private String            hostname;
    private String            diskSlot;
    private Integer           cpuSlotNum = Integer.valueOf(1);
    private Integer           memSlotNum = Integer.valueOf(1);
    private Integer           port;

    public String getSupervisorId() {
        return supervisorId;
    }

    public void setSupervisorId(String supervisorId) {
        this.supervisorId = supervisorId;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getDiskSlot() {
        return diskSlot;
    }

    public void setDiskSlot(String diskSlot) {
        this.diskSlot = diskSlot;
    }

    public Integer getCpuSlotNum() {
        return cpuSlotNum;
    }

    public void setCpuSlotNum(Integer cpuSlotNum) {
        this.cpuSlotNum = cpuSlotNum;
    }

    public Integer getMemSlotNum() {
        return memSlotNum;
    }

    public void setMemSlotNum(Integer memSlotNum) {
        this.memSlotNum = memSlotNum;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cpuSlotNum == null) ? 0 : cpuSlotNum.hashCode());
        result = prime * result + ((diskSlot == null) ? 0 : diskSlot.hashCode());
        result = prime * result + ((memSlotNum == null) ? 0 : memSlotNum.hashCode());
        result = prime * result + ((port == null) ? 0 : port.hashCode());
        result = prime * result + ((supervisorId == null) ? 0 : supervisorId.hashCode());
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
        ResourceAssignment other = (ResourceAssignment) obj;
        if (cpuSlotNum == null) {
            if (other.cpuSlotNum != null)
                return false;
        } else if (!cpuSlotNum.equals(other.cpuSlotNum))
            return false;
        if (diskSlot == null) {
            if (other.diskSlot != null)
                return false;
        } else if (!diskSlot.equals(other.diskSlot))
            return false;
        if (memSlotNum == null) {
            if (other.memSlotNum != null)
                return false;
        } else if (!memSlotNum.equals(other.memSlotNum))
            return false;
        if (port == null) {
            if (other.port != null)
                return false;
        } else if (!port.equals(other.port))
            return false;
        if (supervisorId == null) {
            if (other.supervisorId != null)
                return false;
        } else if (!supervisorId.equals(other.supervisorId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public String toJSONString() {
        StringBuilder sb = new StringBuilder();
        
        sb.append("[");
        sb.append("\"" + supervisorId + "\"");
        sb.append(",");
        sb.append("\"" + hostname+ "\"");
        sb.append(",");
        sb.append("\"" + diskSlot + "\"");
        sb.append(",");
        sb.append("\"" + String.valueOf(cpuSlotNum) + "\"");
        sb.append(",");
        sb.append("\"" + String.valueOf(memSlotNum) + "\"");
        sb.append(",");
        sb.append("\"" + String.valueOf(port) + "\"");
        sb.append("]");
        
        
        
        return sb.toString();
    }
    
    public static String parseString(String str) {
        return ("null".equals(str)? null: str);
    }
    
    public static Integer parseInteger(String str) {
        return ("null".equals(str)? null: Integer.parseInt(str));
    }
    
    public static ResourceAssignment parseFromObj(Object obj) {
        if (obj == null) {
            return null;
        }
        
        if (obj instanceof List<?> == false) {
            return null;
        }
        
        try {
            List<String> list = (List<String> )obj;
            if (list.size() < 6) {
                return null;
            }
            String supervisorId = parseString(list.get(0));
            String hostname = parseString(list.get(1));
            String diskSlot = parseString(list.get(2));
            Integer cpuSlotNum = parseInteger(list.get(3));
            Integer memSlotNum = parseInteger(list.get(4));
            Integer port = parseInteger(list.get(5));
            
            ResourceAssignment ret = new ResourceAssignment();
            
            ret.setSupervisorId(supervisorId);
            ret.setHostname(hostname);
            ret.setCpuSlotNum(cpuSlotNum);
            ret.setMemSlotNum(memSlotNum);
            ret.setDiskSlot(diskSlot);
            ret.setPort(port);
            return ret;
        }catch(Exception e) {
            return null;
        }
        
    }
    
    public boolean isCpuMemInvalid() {
        if (cpuSlotNum == null || cpuSlotNum <= 0) {
            return true;
        }

        if (memSlotNum == null || memSlotNum <= 0) {
            return true;
        }
        
        return false;
    }
    
    public static void main(String[] args) {
        ResourceAssignment input = new ResourceAssignment();
        
        input.setSupervisorId("1");
        input.setCpuSlotNum(2);
        
        String outString = JSONValue.toJSONString(input);
        
        System.out.println(outString);
        
        Object object = JSONValue.parse(outString);
        
        System.out.println("Class " + object.getClass().getName());
        
        System.out.println(parseFromObj(object));
    }

}
