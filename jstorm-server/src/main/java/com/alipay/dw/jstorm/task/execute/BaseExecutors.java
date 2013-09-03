package com.alipay.dw.jstorm.task.execute;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.client.ConfigExtension;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.daemon.worker.WorkerTransfer;
import com.alipay.dw.jstorm.stats.CommonStatsRolling;
import com.alipay.dw.jstorm.task.TaskStatus;
import com.alipay.dw.jstorm.task.error.ITaskReportErr;
import com.alipay.dw.jstorm.utils.RunCounter;
import com.alipay.dw.jstorm.zeroMq.IRecvConnection;

/**
 * Base executor share between spout and bolt
 * 
 * 
 * @author Longda
 * 
 */
public class BaseExecutors extends RunnableCallback {
    private static Logger           LOG                  = Logger.getLogger(BaseExecutors.class);
    
    protected final String          component_id;
    protected final int             taskId;
    protected final boolean         isDebugRecv;
    protected final boolean         isDebug;
    protected final String          idStr;
    
    protected Map                   storm_conf;
    // ZMQConnection puller
    protected IRecvConnection       puller;
    
    protected TopologyContext       userTopologyCtx;
    protected CommonStatsRolling    task_stats;
    
    protected KryoTupleDeserializer deserializer;
    
    protected TaskStatus            taskStatus;
    
    protected int                   message_timeout_secs = 30;
    
    protected Exception             error                = null;
    
    ITaskReportErr                  report_error;
    
    
    //protected IntervalCheck         intervalCheck = new IntervalCheck();
    
    public BaseExecutors(WorkerTransfer _transfer_fn, Map _storm_conf,
            IRecvConnection _puller, TopologyContext topology_context,
            TopologyContext _user_context, CommonStatsRolling _task_stats,
            TaskStatus taskStatus, ITaskReportErr _report_error) {
        
        this.storm_conf = _storm_conf;
        this.puller = _puller;
        
        this.userTopologyCtx = _user_context;
        this.task_stats = _task_stats;
        this.taskId = topology_context.getThisTaskId();
        this.component_id = topology_context.getThisComponentId();
        this.idStr = "ComponentId:" + component_id + ",taskId:" + taskId + " ";
        
        this.taskStatus = taskStatus;
        this.report_error = _report_error;
        
        this.deserializer = new KryoTupleDeserializer(storm_conf,
                topology_context);// (KryoTupleDeserializer.
        
        this.isDebugRecv = JStormUtils.parseBoolean(
                storm_conf.get(ConfigExtension.TOPOLOGY_DEBUG_RECV_TUPLE),
                false);
        this.isDebug = JStormUtils.parseBoolean(
                storm_conf.get(Config.TOPOLOGY_DEBUG), false);
        
        message_timeout_secs = JStormUtils.parseInt(storm_conf
                .get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
        
        
    }
    
    protected Tuple recv() {
        
        try {
            byte[] ser_msg = puller.recv();
            
            if (ser_msg == null) {
                return null;
            }
            
            if (ser_msg.length == 0) {
                return null;
            } else if (ser_msg.length == 1) {
                byte newStatus = ser_msg[0];
                LOG.info("Change task status as " + newStatus);
                taskStatus.setStatus(newStatus);
                
                if (newStatus == TaskStatus.SHUTDOWN) {
                    puller.close();
                }
                return null;
            }
            
            // ser_msg.length > 1
            Tuple tuple = deserializer.deserialize(ser_msg);
            
            if (isDebugRecv) {
                
                LOG.info(idStr + " receive " + tuple.toString());
            }
            
            //recv_tuple_queue.offer(tuple);
            
            task_stats.recv_tuple(tuple.getSourceComponent(),
                    tuple.getSourceStreamId());
            
            
            return tuple;
            
        } catch (Exception e) {
            LOG.error("Recv thread error:" + idStr, e);
        }
        
        return null;
    }
    
    @Override
    public void run() {
        // this function will be override by SpoutExecutor or BoltExecutor
        LOG.info("BaseExector run");
    }
    
    @Override
    public Object getResult() {
        if (taskStatus.isRun()) {
            return 0;
        } else if (taskStatus.isPause()) {
            return 0;
        } else if (taskStatus.isShutdown()) {
            LOG.info("Shutdown executing thread of " + idStr);
            return -1;
        } else {
            LOG.info("Unknow TaskStatus, shutdown executing thread of " + idStr);
            return -1;
        }
    }
    
    
    @Override
    public Exception error() {
        return error;
    }
    
    
}
