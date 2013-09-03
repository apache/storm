package com.alipay.dw.jstorm.task.execute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;

import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.daemon.worker.WorkerTransfer;
import com.alipay.dw.jstorm.stats.CommonStatsRolling;
import com.alipay.dw.jstorm.task.acker.Acker;
import com.alipay.dw.jstorm.task.comm.TaskSendTargets;
import com.alipay.dw.jstorm.task.comm.UnanchoredSend;
import com.alipay.dw.jstorm.task.error.ITaskReportErr;
import com.alipay.dw.jstorm.utils.EventSampler;
import com.alipay.dw.jstorm.utils.TimeUtils;

/**
 * bolt output interface, do emit/ack/fail
 * 
 * @author yannian/Longda
 * 
 */
public class BoltCollector implements IOutputCollector {
    private static Logger             LOG = Logger.getLogger(BoltCollector.class);
    
    private ITaskReportErr            reportError;
    private TaskSendTargets           sendTargets;
    private WorkerTransfer            workerTransfer;
    private TopologyContext           topologyContext;
    private Integer                   task_id;
    private TimeCacheMap<Tuple, Long> tuple_start_times;
    private CommonStatsRolling        task_stats;
    private TimeCacheMap<Tuple, Long> pending_acks;
    private Map                       storm_conf;
    private Integer                   ackerNum;
    
    
    
    
    
    public BoltCollector(int message_timeout_secs, ITaskReportErr report_error,
            TaskSendTargets _send_fn, Map _storm_conf,
            WorkerTransfer _transfer_fn, TopologyContext _topology_context,
            Integer task_id, TimeCacheMap<Tuple, Long> tuple_start_times,
            CommonStatsRolling _task_stats) {
        
        this.reportError = report_error;
        this.sendTargets = _send_fn;
        this.storm_conf = _storm_conf;
        this.workerTransfer = _transfer_fn;
        this.topologyContext = _topology_context;
        this.task_id = task_id;
        this.task_stats = _task_stats;
        
        this.pending_acks = new TimeCacheMap<Tuple, Long>(message_timeout_secs, 
                Acker.TIMEOUT_BUCKET_NUM);
        this.tuple_start_times = tuple_start_times;
        
        this.ackerNum = JStormUtils.parseInt(storm_conf
                .get(Config.TOPOLOGY_ACKERS));
        
    }
    
    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors,
            List<Object> tuple) {
        return boltEmit(streamId, anchors, tuple, null);
    }
    
    @Override
    public void emitDirect(int taskId, String streamId,
            Collection<Tuple> anchors, List<Object> tuple) {
        boltEmit(streamId, anchors, tuple, taskId);
    }
    
    private List<Integer> boltEmit(String out_stream_id,
            Collection<Tuple> anchors, List<Object> values, Integer out_task_id) {
        try {
            java.util.List<Integer> out_tasks = null;
            if (out_task_id != null) {
                out_tasks = sendTargets.get(out_task_id, out_stream_id, values);
            } else {
                out_tasks = sendTargets.get(out_stream_id, values);
            }
            
            for (Integer t : out_tasks) {
                Map<Long, Long> anchors_to_ids = new HashMap<Long, Long>();
                if (anchors != null) {
                    for (Tuple a : anchors) {
                        Long edge_id = MessageId.generateId();
                        put_xor(pending_acks, a, edge_id);
                        for (Long root_id : a.getMessageId().getAnchorsToIds()
                                .keySet()) {
                            put_xor(anchors_to_ids, root_id, edge_id);
                        }
                    }
                }
                MessageId msgid = MessageId.makeId(anchors_to_ids);
                workerTransfer.transfer(t, new Tuple(topologyContext, values,
                        task_id, out_stream_id, msgid));
                
            }
            return out_tasks;
        } catch (Exception e) {
            LOG.error("bolt emit", e);
        }
        return new ArrayList<Integer>();
    }
    
    @Override
    public void ack(Tuple input) {
        
        if (ackerNum > 0) {
            
            Long ack_val = Long.valueOf(0);
            Object pend_val = pending_acks.remove(input);
            if (pend_val != null) {
                ack_val = (Long)(pend_val);
            }
            
            for (Entry<Long, Long> e : input.getMessageId().getAnchorsToIds()
                    .entrySet()) {
                
                UnanchoredSend.send(
                        topologyContext,
                        sendTargets,
                        workerTransfer,
                        Acker.ACKER_ACK_STREAM_ID,
                        JStormUtils.mk_list((Object) e.getKey(),
                                JStormUtils.bit_xor(e.getValue(), ack_val)));
            }
        }
        
        Long delta = tuple_time_delta(tuple_start_times, input);
        if (delta != null) {
            task_stats.bolt_acked_tuple(input.getSourceComponent(),
                    input.getSourceStreamId(), delta);
        }
    }
    
    @Override
    public void fail(Tuple input) {
        // if ackerNum == 0, we can just return
        if (ackerNum > 0) {
            pending_acks.remove(input);
            for (Entry<Long, Long> e : input.getMessageId().getAnchorsToIds()
                    .entrySet()) {
                UnanchoredSend.send(topologyContext, sendTargets, workerTransfer,
                        Acker.ACKER_FAIL_STREAM_ID,
                        JStormUtils.mk_list((Object) e.getKey()));
            }
        }
        

       task_stats.bolt_failed_tuple(input.getSourceComponent(),
                        input.getSourceStreamId());
 
    }
    
    @Override
    public void reportError(Throwable error) {
        reportError.report(error);
    }
    
    // Utility functions, just used here
    public static Long tuple_time_delta(TimeCacheMap<Tuple, Long> start_times,
            Tuple tuple) {
        Long start_time = (Long) start_times.remove(tuple);
        if (start_time != null) {
            return TimeUtils.time_delta_ms(start_time);
        }
        return null;
    }
    
    public static void put_xor(TimeCacheMap<Tuple, Long> pending, Tuple key,
            Long id) {
        // synchronized (pending) {
        Long curr = pending.get(key);
        if (curr == null) {
            curr = Long.valueOf(0);
        }
        pending.put(key, JStormUtils.bit_xor(curr, id));
        // }
    }
    
    public static void put_xor(Map<Long, Long> pending, Long key, Long id) {
        // synchronized (pending) {
        Long curr = pending.get(key);
        if (curr == null) {
            curr = Long.valueOf(0);
        }
        pending.put(key, JStormUtils.bit_xor(curr, id));
        // }
    }
    
}
