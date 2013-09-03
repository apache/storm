package com.alipay.dw.jstorm.daemon.worker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import backtype.storm.Config;

import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.cluster.StormBase;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.daemon.nimbus.StatusType;
import com.alipay.dw.jstorm.task.TaskShutdownDameon;
import com.alipay.dw.jstorm.common.JStormUtils;

/**
 * Timely check whether topology is active or not from ZK
 * 
 * @author yannian/Longda
 * 
 */
public class RefreshActive extends RunnableCallback {
    private static Logger       LOG = Logger.getLogger(RefreshActive.class);
    
    private WorkerData          workerData;
    
    private AtomicBoolean       active;
    private Map<Object, Object> conf;
    private StormClusterState   zkCluster;
    private String              topologyId;
    private Integer             frequence;
    
    // private Object lock = new Object();
    
    @SuppressWarnings("rawtypes")
    public RefreshActive(WorkerData workerData) {
        this.workerData = workerData;
        
        this.active = workerData.getActive();
        this.conf = workerData.getConf();
        this.zkCluster = workerData.getZkCluster();
        this.topologyId = workerData.getTopologyId();
        this.frequence = JStormUtils.parseInt(
                conf.get(Config.TASK_REFRESH_POLL_SECS), 10);
    }
    
    @Override
    public void run() {
        
        if (active.get() == false) {
            return ;
        }
        
        try {
            StatusType newTopologyStatus = StatusType.activate;
            // /ZK-DIR/topology
            StormBase base = zkCluster.storm_base(topologyId, this);
            if (base == null) {
                // @@@ normally the topology has been removed
                LOG.warn("Failed to get StromBase from ZK of " + topologyId);
                newTopologyStatus = StatusType.killed;
            } else {
                
                newTopologyStatus = base.getStatus().getStatusType();
            }
            
            StatusType oldTopologyStatus = workerData.getTopologyStatus();
            
            if (newTopologyStatus.equals(oldTopologyStatus)) {
                return;
            }
            
            LOG.info("Old TopologyStatus:" + oldTopologyStatus + 
                    ", new TopologyStatus:" + newTopologyStatus);
            
            workerData.setTopologyStatus(newTopologyStatus);
            
            if (newTopologyStatus.equals(StatusType.active)) {
                List<TaskShutdownDameon> tasks = workerData.getShutdownTasks();
                for (TaskShutdownDameon task : tasks) {
                    task.active();
                }
            } else {
                List<TaskShutdownDameon> tasks = workerData.getShutdownTasks();
                for (TaskShutdownDameon task : tasks) {
                    task.deactive();
                }
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("Failed to get topology from ZK ", e);
            return;
        }
        
    }
    
    @Override
    public Object getResult() {
        if (active.get()) {
            return frequence;
        }
        return -1;
    }
}
