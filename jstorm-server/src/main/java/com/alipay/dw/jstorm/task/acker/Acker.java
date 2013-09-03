package com.alipay.dw.jstorm.task.acker;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;

import com.alipay.dw.jstorm.common.JStormUtils;

/**
 * 
 * @author yannian/Longda
 * 
 */
public class Acker implements IBolt {
    
    private static final Logger             LOG                  = Logger.getLogger(Acker.class);
    
    private static final long               serialVersionUID     = 4430906880683183091L;
    
    public static final String              ACKER_COMPONENT_ID   = "__acker";
    public static final String              ACKER_INIT_STREAM_ID = "__ack_init";
    public static final String              ACKER_ACK_STREAM_ID  = "__ack_ack";
    public static final String              ACKER_FAIL_STREAM_ID = "__ack_fail";
    
    public static final int                 TIMEOUT_BUCKET_NUM   = 3;
    
    private OutputCollector                 collector            = null;
    private TimeCacheMap<Object, AckObject> pending              = null;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        String key = Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS;
        int timeoutSec = JStormUtils.parseInt(stormConf.get(key), 30);
        pending = new TimeCacheMap<Object, AckObject>(timeoutSec,
                TIMEOUT_BUCKET_NUM);
    }
    
    @Override
    public void execute(Tuple input) {
        Object id = input.getValue(0);
        
        AckObject curr = pending.get(id);
        
        String stream_id = input.getSourceStreamId();
        
        if (Acker.ACKER_INIT_STREAM_ID.equals(stream_id)) {
            if (curr == null) {
                curr = new AckObject();
                
                curr.val = input.getLong(1);
                curr.spout_task = input.getInteger(2);
                
                pending.put(id, curr);
            } else {
                // bolt's ack first come
                curr.update_ack(input.getValue(1));
                curr.spout_task = input.getInteger(2);
            }
            
        } else if (Acker.ACKER_ACK_STREAM_ID.equals(stream_id)) {
            if (curr != null) {
                curr.update_ack(input.getValue(1));
                
            } else {
                // two case
                // one is timeout
                // the other is bolt's ack first come
                curr = new AckObject();
                
                curr.val = Long.valueOf(input.getLong(1));
                
                pending.put(id, curr);
                
            }
        } else if (Acker.ACKER_FAIL_STREAM_ID.equals(stream_id)) {
            if (curr == null) {
                // do nothing
                // already timeout, should go fail
                return;
            }
            
            curr.failed = true;
            
        } else {
            LOG.info("Unknow source stream");
            return;
        }
        
        Integer task = curr.spout_task;
        
        if (task != null) {
            
            if (curr.val == 0) {
                pending.remove(id);
                List values = JStormUtils.mk_list(id);
                
                collector.emitDirect(task, Acker.ACKER_ACK_STREAM_ID, values);
                
            } else {
                
                if (curr.failed) {
                    pending.remove(id);
                    List values = JStormUtils.mk_list(id);
                    collector.emitDirect(task, Acker.ACKER_FAIL_STREAM_ID,
                            values);
                }
            }
        } else {
            
        }
        
        // add this operation to update acker's ACK statics
        collector.ack(input);
        
    }
    
    @Override
    public void cleanup() {
        LOG.info("Successfully cleanup");
    }
    
}
