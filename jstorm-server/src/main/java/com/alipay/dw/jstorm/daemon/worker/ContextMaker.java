package com.alipay.dw.jstorm.daemon.worker;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;

import com.alipay.dw.jstorm.cluster.StormConfig;

/**
 * ContextMaker
 * This class is used to create TopologyContext
 * 
 * @author yannian/Longda
 * 
 */
public class ContextMaker {
    private static Logger            LOG = Logger.getLogger(ContextMaker.class);
    
    private Map                      stormConf;
    private String                   topologyId;
    private String                   workerId;
    private HashMap<Integer, String> tasksToComponent;
    private String                   resourcePath;
    private String                   workPid;
    private Integer                  port;
    private List<Integer>            workerTasks;
    
    @SuppressWarnings("rawtypes")
    public ContextMaker(Map stormConf, String topologyId, String workerId,
            HashMap<Integer, String> tasksToComponent, Integer port,
            List<Integer> workerTasks) {
        this.stormConf = stormConf;
        this.topologyId = topologyId;
        this.workerId = workerId;
        this.tasksToComponent = tasksToComponent;
        this.port = port;
        this.workerTasks = workerTasks;
        
        try {
            String distroot = StormConfig.supervisor_stormdist_root(stormConf,
                    topologyId);
            
            resourcePath = StormConfig
                    .supervisor_storm_resources_path(distroot);
            
            workPid = StormConfig.worker_pids_root(stormConf, workerId);
            
        } catch (IOException e) {
            LOG.error("Failed to create ContextMaker", e);
            throw new RuntimeException(e);
        }
    }
    
    public TopologyContext makeTopologyContext(StormTopology topology,
            Integer taskId) {
        return new TopologyContext(topology, stormConf, tasksToComponent,
                topologyId, resourcePath, workPid, taskId, port, workerTasks);
        
        
    }
    
}
