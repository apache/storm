package com.alipay.dw.jstorm.daemon.worker;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.daemon.Shutdownable;

import com.alipay.dw.jstorm.callback.AsyncLoopThread;
import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.task.comm.VirtualPortDispatch;
import com.alipay.dw.jstorm.task.comm.VirtualPortShutdown;
import com.alipay.dw.jstorm.utils.JStormServerUtils;
import com.alipay.dw.jstorm.zeroMq.IRecvConnection;
import com.alipay.dw.jstorm.zeroMq.MQContext;

/**
 * worker receive tuple dispatcher
 * 
 * @author yannian/Longda
 * 
 */
public class WorkerVirtualPort {
    
    private final static Logger LOG = Logger.getLogger(WorkerVirtualPort.class);
    
    @SuppressWarnings("rawtypes")
    private Map                 conf;
    private String              supervisorId;
    private Integer             port;
    private MQContext           mqContext;
    private Set<Integer>        taskIds;
    private String              topologyId;
    
    public WorkerVirtualPort(WorkerData workerData) {
        //
        //Map conf, String supervisor_id, String storm_id,
        //Integer port, MQContext mq_context, Set<Integer> task_ids
        this.conf = workerData.getStormConf();
        this.supervisorId = workerData.getSupervisorId();
        this.port = workerData.getPort();
        this.mqContext = workerData.getMqContext();
        this.taskIds = workerData.getTaskids();
        this.topologyId = workerData.getTopologyId();
    }
    
    public Shutdownable launch() throws InterruptedException {
        
        String msg = "Launching virtual port for supervisor";
        LOG.info(msg + ":" + supervisorId + " stormid:" + topologyId + " port:"
                + port);
        
        boolean islocal = StormConfig.local_mode(conf);
        
        RunnableCallback killfn = JStormServerUtils.getDefaultKillfn();
        
        IRecvConnection recvConnection = mqContext.bind(true, port);
        
        RunnableCallback recvDispather = new VirtualPortDispatch(mqContext,
                recvConnection, taskIds);
        
        AsyncLoopThread vthread = new AsyncLoopThread(recvDispather, false,
                killfn, Thread.MAX_PRIORITY, true);
        
        return new VirtualPortShutdown(mqContext, vthread, port);
        
    }
    
}
