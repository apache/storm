package com.alipay.dw.jstorm.daemon.supervisor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.utils.TimeUtils;

/**
 * supervisor Heartbeat, just write SupervisorInfo to ZK
 */
class Heartbeat extends RunnableCallback {
    
    private static Logger       LOG = Logger.getLogger(Heartbeat.class);
    
    private Map<Object, Object> conf;
    
    private StormClusterState   stormClusterState;
    
    private String              supervisorId;
    
    private String              myHostName;
    
    private int                 startTime;
    
    private int                 frequence;
    
    
    /**
     * @param conf
     * @param stormClusterState
     * @param supervisorId
     * @param myHostName
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Heartbeat(Map conf, StormClusterState stormClusterState,
            String supervisorId, String myHostName, int startTimeStamp,
            AtomicBoolean active) {
        this.stormClusterState = stormClusterState;
        this.supervisorId = supervisorId;
        this.conf = conf;
        this.myHostName = myHostName;
        this.startTime = startTimeStamp;
        this.active = active;
        this.frequence = JStormUtils.parseInt(conf
                .get(Config.SUPERVISOR_HEARTBEAT_FREQUENCY_SECS));
    }
    
    @SuppressWarnings("unchecked")
    public void update() {
        SupervisorInfo sInfo = new SupervisorInfo(
                TimeUtils.current_time_secs(), myHostName,
                (List<Integer>) conf.get(Config.SUPERVISOR_SLOTS_PORTS),
                (int) (TimeUtils.current_time_secs() - startTime));
        
        try {
            stormClusterState.supervisor_heartbeat(supervisorId, sInfo);
        } catch (Exception e) {
            LOG.error("Failed to update SupervisorInfo to ZK", e);
            
        }
    }
    
    private AtomicBoolean active = null;
    
    private Integer       result;
    
    @Override
    public Object getResult() {
        return result;
    }
    
    @Override
    public void run() {
        update();
        if (active.get()) {
            result = frequence;
        } else {
            result = -1;
        }
    }
    
}
