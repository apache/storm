package com.alipay.dw.jstorm.task.execute;

import org.apache.log4j.Logger;

import backtype.storm.spout.ISpout;

import com.alipay.dw.jstorm.client.spout.IFailValueSpout;
import com.alipay.dw.jstorm.stats.CommonStatsRolling;
import com.alipay.dw.jstorm.task.comm.TupleInfo;

/**
 * Do the action after spout receive one failed tuple or sending tuple timeout
 * 
 * @author yannian/Longda
 * 
 */
public class FailSpoutMsg implements Runnable {
    private static Logger      LOG     = Logger.getLogger(FailSpoutMsg.class);
    private ISpout             spout;
    private TupleInfo          tupleInfo;
    private CommonStatsRolling task_stats;
    private boolean     isDebug = false;

    
    public FailSpoutMsg(ISpout _spout, TupleInfo _tupleInfo, 
            CommonStatsRolling _task_stats, boolean _isDebug) {
        this.spout = _spout;
        this.tupleInfo = _tupleInfo;
        this.task_stats = _task_stats;
        this.isDebug = _isDebug;
    }
    
    public void run() {
        Object msg_id = tupleInfo.getMessageId();
        
        if (spout instanceof IFailValueSpout) {
            IFailValueSpout enhanceSpout = (IFailValueSpout)spout;
            enhanceSpout.fail(msg_id, tupleInfo.getValues());
        }else {
            spout.fail(msg_id);
        }
        
        task_stats.spout_failed_tuple(tupleInfo.getStream());
        
        if (isDebug ) {
            LOG.info("Failed message " + msg_id + ": " + tupleInfo.getValues().toString());
        }
    }
    
}
