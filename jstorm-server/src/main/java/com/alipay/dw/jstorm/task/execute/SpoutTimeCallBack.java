package com.alipay.dw.jstorm.task.execute;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.TimeCacheMap;

import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.stats.CommonStatsRolling;
import com.alipay.dw.jstorm.task.comm.TupleInfo;

/**
 * Spout tuple Timeout callback in the Spout Executor's TimeoutCache
 * 
 * @author yannian
 * 
 * @param <K>
 * @param <V>
 */
public class SpoutTimeCallBack<K, V> implements
        TimeCacheMap.ExpiredCallback<K, V> {
    private static Logger                   LOG = Logger.getLogger(SpoutTimeCallBack.class);
    
    private LinkedBlockingQueue<Runnable> event_queue;
    private backtype.storm.spout.ISpout     spout;
    private Map                             storm_conf;
    private CommonStatsRolling              task_stats;
    private boolean                         isDebug;
    
    public SpoutTimeCallBack(LinkedBlockingQueue<Runnable> _event_queue,
            backtype.storm.spout.ISpout _spout, Map _storm_conf,
            CommonStatsRolling stat) {
        this.event_queue = _event_queue;
        this.spout = _spout;
        this.storm_conf = _storm_conf;
        this.task_stats = stat;
        this.isDebug  = JStormUtils.parseBoolean(
                storm_conf.get(Config.TOPOLOGY_DEBUG), false);
    }
    
    /**
     * pending.put(root_id, JStormUtils.mk_list(message_id, TupleInfo, ms));
     */
    @Override
    public void expire(K key, V val) {
        if (val == null) {
            return;
        }
        try {
            TupleInfo  tupleInfo = (TupleInfo)val;
            
            event_queue.offer(new FailSpoutMsg(spout, (TupleInfo) tupleInfo, 
                    task_stats, isDebug));
        } catch (Exception e) {
            LOG.error("expire error", e);
        }
    }
}
