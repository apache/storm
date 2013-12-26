package com.alibaba.jstorm.daemon.supervisor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.jboss.netty.util.internal.SharedResourceMisuseDetector;

import com.alibaba.jstorm.resource.ResourceAssignment;
import com.alibaba.jstorm.resource.SharedResourcePool;
import com.alibaba.jstorm.resource.SlotResourcePool;

/**
 * Object stored in ZK /ZK-DIR/supervisors
 * 
 * @author Xin.Zhou/Longda
 */
public class SupervisorInfo implements Serializable {

    private static final long         serialVersionUID = -8384417078907518922L;

    private final String              hostName;

    private Integer                   timeSecs;
    private Integer                   uptimeSecs;

    //@@@ Todo, in the next step, it should abstract resource type
    // here just hard code the cpu/mem/disk/net resource
    private SharedResourcePool        cpuPool;
    private SharedResourcePool        memPool;
    private SlotResourcePool<String>  diskPool;
    private SlotResourcePool<Integer> netPool;

    public SupervisorInfo(String hostName) {
        this.hostName = hostName;
    }

    public String getHostName() {
        return hostName;
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

    public SharedResourcePool getCpuPool() {
        return cpuPool;
    }

    public void setCpuPool(SharedResourcePool cpuPool) {
        this.cpuPool = cpuPool;
    }

    public SharedResourcePool getMemPool() {
        return memPool;
    }

    public void setMemPool(SharedResourcePool memPool) {
        this.memPool = memPool;
    }

    public SlotResourcePool<String> getDiskPool() {
        return diskPool;
    }

    public void setDiskPool(SlotResourcePool<String> diskPool) {
        this.diskPool = diskPool;
    }

    public SlotResourcePool<Integer> getNetPool() {
        return netPool;
    }

    public void setNetPool(SlotResourcePool<Integer> netPool) {
        this.netPool = netPool;
    }

    @Override
    public boolean equals(Object hb) {
        if (hb instanceof SupervisorInfo

        && ((SupervisorInfo) hb).timeSecs.equals(timeSecs)
            && ((SupervisorInfo) hb).hostName.equals(hostName)
            && ((SupervisorInfo) hb).cpuPool.equals(cpuPool)
            && ((SupervisorInfo) hb).memPool.equals(memPool)
            && ((SupervisorInfo) hb).diskPool.equals(diskPool)
            && ((SupervisorInfo) hb).netPool.equals(netPool)
            && ((SupervisorInfo) hb).uptimeSecs.equals(uptimeSecs)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return timeSecs.hashCode() + uptimeSecs.hashCode() + hostName.hashCode()
               + cpuPool.hashCode() + memPool.hashCode() + diskPool.hashCode() + netPool.hashCode();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    /**
     * get Map<supervisorId, hostname>
     * 
     * @param stormClusterState
     * @param callback
     * @return
     */
    public static Map<String, String> getNodeHost(Map<String, SupervisorInfo> supInfos) {

        Map<String, String> rtn = new HashMap<String, String>();

        for (Entry<String, SupervisorInfo> entry : supInfos.entrySet()) {

            SupervisorInfo superinfo = entry.getValue();

            String supervisorid = entry.getKey();

            rtn.put(supervisorid, superinfo.getHostName());

        }

        return rtn;
    }
    
    public void allocResource(ResourceAssignment resource) {
        cpuPool.alloc(resource.getCpuSlotNum(), null);
        memPool.alloc(resource.getMemSlotNum(), null);
        diskPool.alloc(resource.getDiskSlot(), null);
        netPool.alloc(resource.getPort(), null);
    }

}
