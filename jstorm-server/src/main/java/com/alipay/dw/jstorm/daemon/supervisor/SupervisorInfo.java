package com.alipay.dw.jstorm.daemon.supervisor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Object stored in ZK /ZK-DIR/supervisors
 * 
 * @author Xin.Zhou/Longda
 */
public class SupervisorInfo implements Serializable {
    
    private static final long serialVersionUID = -8384417078907518922L;
    private Integer           timeSecs;
    private String            hostName;
    private List<Integer>     workPorts;
    private Integer           uptimeSecs;
    
    public SupervisorInfo(int timeSecs, String hostName,
            List<Integer> workPorts, int uptimeSecs) {
        this.timeSecs = timeSecs;
        this.hostName = hostName;
        this.workPorts = workPorts;
        this.uptimeSecs = uptimeSecs;
    }
    
    public int getTimeSecs() {
        return timeSecs;
    }
    
    public void setTimeSecs(int timeSecs) {
        this.timeSecs = timeSecs;
    }
    
    public String getHostName() {
        return hostName;
    }
    
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
    
    public List<Integer> getWorkPorts() {
        return workPorts;
    }
    
    public void setWorkPorts(List<Integer> workPorts) {
        this.workPorts = workPorts;
    }
    
    public int getUptimeSecs() {
        return uptimeSecs;
    }
    
    public void setUptimeSecs(int uptimeSecs) {
        this.uptimeSecs = uptimeSecs;
    }
    
    @Override
    public boolean equals(Object hb) {
        if (hb instanceof SupervisorInfo
        
        && ((SupervisorInfo) hb).timeSecs.equals(timeSecs)
                && ((SupervisorInfo) hb).hostName.equals(hostName)
                && ((SupervisorInfo) hb).workPorts.equals(workPorts)
                && ((SupervisorInfo) hb).uptimeSecs.equals(uptimeSecs)) {
            return true;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return timeSecs.hashCode() + uptimeSecs.hashCode()
                + hostName.hashCode() + workPorts.hashCode();
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
