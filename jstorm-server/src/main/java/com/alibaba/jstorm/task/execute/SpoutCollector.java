package com.alibaba.jstorm.task.execute;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.stats.CommonStatsRolling;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.comm.UnanchoredSend;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RotatingMap;
import com.lmax.disruptor.InsufficientCapacityException;

/**
 * spout collector, sending tuple through this Object
 * 
 * @author yannian/Longda
 * 
 */
public class SpoutCollector implements ISpoutOutputCollector {
    private static Logger                   LOG = Logger.getLogger(SpoutCollector.class);
    
    private TaskSendTargets                 sendTargets;
    private Map                             storm_conf;
    private TaskTransfer                    transfer_fn;
//    private TimeCacheMap                    pending;
    private RotatingMap<Long, TupleInfo>    pending;
    // topology_context is system topology context
    private TopologyContext                 topology_context;
    
    private DisruptorQueue disruptorAckerQueue;   
    private CommonStatsRolling              task_stats;
    private backtype.storm.spout.ISpout     spout;
    private ITaskReportErr                  report_error;
    
    private Integer                         task_id;
    private Integer                         ackerNum;
    private boolean                         isDebug = false;
    
    
    
    public SpoutCollector(Integer task_id, backtype.storm.spout.ISpout spout,
            CommonStatsRolling task_stats, TaskSendTargets sendTargets,
            Map _storm_conf, TaskTransfer _transfer_fn, 
            RotatingMap<Long, TupleInfo> pending,
            TopologyContext topology_context,
            DisruptorQueue disruptorAckerQueue,
            ITaskReportErr _report_error) {
        this.sendTargets = sendTargets;
        this.storm_conf = _storm_conf;
        this.transfer_fn = _transfer_fn;
        this.pending = pending;
        this.topology_context = topology_context;
        
        this.disruptorAckerQueue = disruptorAckerQueue;
        
        
        
        this.task_stats = task_stats;
        this.spout = spout;
        this.task_id = task_id;
        this.report_error = _report_error;
        
        ackerNum = JStormUtils.parseInt(storm_conf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
        isDebug = JStormUtils.parseBoolean(
                storm_conf.get(Config.TOPOLOGY_DEBUG), false);
    }
    
    @Override
    public List<Integer> emit(String streamId, List<Object> tuple,
            Object messageId) {
        return sendSpoutMsg(streamId, tuple, messageId, null);
    }
    
    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple,
            Object messageId) {
        sendSpoutMsg(streamId, tuple, messageId, taskId);
    }
    
    private List<Integer> sendSpoutMsg(String out_stream_id,
            List<Object> values, Object message_id, Integer out_task_id) {
        
        java.util.List<Integer> out_tasks = null;
        if (out_task_id != null) {
            out_tasks = sendTargets.get(out_task_id, out_stream_id, values);
        } else {
            out_tasks = sendTargets.get(out_stream_id, values);
        }
        
        if (out_tasks.size() == 0) {
            //don't need send tuple to other task
            return out_tasks;
        }
        
        Long root_id = MessageId.generateId();
        
        Boolean needAck = (message_id != null) && (ackerNum > 0);
        
        for (Integer t : out_tasks) {
            MessageId msgid;
            if (needAck) {
                msgid = MessageId.makeRootId(root_id, t);
            } else {
                msgid = MessageId.makeUnanchored();
            }
            
            TupleImplExt tp = new TupleImplExt(topology_context, values, task_id,
                    out_stream_id, msgid);
            tp.setTargetTaskId(t);
            transfer_fn.transfer(tp);
            
        }
        
        if (needAck) {
        	
            TupleInfo info = new TupleInfo();
            info.setStream(out_stream_id);
            info.setValues(values);
            info.setMessageId(message_id);
            info.setTimestamp(System.currentTimeMillis());
            pending.putHead(root_id, info);
            
            
            List<Object> ackerTuple = JStormUtils.mk_list((Object) root_id,
                    JStormUtils.bit_xor_vals(out_tasks), task_id);
            
            UnanchoredSend.send(topology_context, sendTargets, transfer_fn,
                    Acker.ACKER_INIT_STREAM_ID, ackerTuple);
            
        } else if (message_id != null) {
            AckSpoutMsg ack = new AckSpoutMsg(spout, message_id, isDebug,
            		out_stream_id, 0, task_stats);
    		
            try {
                //@@@ Change the logic 
                // change tryPublish to public
    			disruptorAckerQueue.publish(ack);
    		} catch (Exception e) {
    			LOG.warn("Acker Queue full", e);
    		}
        }
        
        return out_tasks;
    }

    @Override
    public void reportError(Throwable error) {
        // TODO Auto-generated method stub
        report_error.report(error);
    }
    
}
