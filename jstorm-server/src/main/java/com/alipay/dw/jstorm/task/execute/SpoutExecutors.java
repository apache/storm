package com.alipay.dw.jstorm.task.execute;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;

import com.alipay.dw.jstorm.client.ConfigExtension;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.daemon.worker.WorkerTransfer;
import com.alipay.dw.jstorm.stats.CommonStatsRolling;
import com.alipay.dw.jstorm.task.TaskStatus;
import com.alipay.dw.jstorm.task.acker.Acker;
import com.alipay.dw.jstorm.task.comm.TaskSendTargets;
import com.alipay.dw.jstorm.task.comm.TupleInfo;
import com.alipay.dw.jstorm.task.error.ITaskReportErr;
import com.alipay.dw.jstorm.utils.RunCounter;
import com.alipay.dw.jstorm.zeroMq.IRecvConnection;

/**
 * spout executor
 * 
 * All spout actions will be done here
 * 
 * @author yannian/Longda
 * 
 */
public class SpoutExecutors extends BaseExecutors {
    private static Logger                   LOG            = Logger.getLogger(SpoutExecutors.class);
    
    protected final Integer                 max_spout_pending;
    protected final boolean                 supportRecvThread;
    
    protected backtype.storm.spout.ISpout   spout;
    protected TimeCacheMap                  pending;
    protected ISpoutOutputCollector         output_collector;
    
    protected LinkedBlockingQueue<Runnable> ackerQueue;
    
    private boolean                         firstTime      = true;
    
    public SpoutExecutors(backtype.storm.spout.ISpout _spout,
            WorkerTransfer _transfer_fn, Map _storm_conf,
            IRecvConnection _puller, TaskSendTargets sendTargets,
            TaskStatus taskStatus, TopologyContext topology_context,
            TopologyContext _user_context, CommonStatsRolling _task_stats,
            ITaskReportErr _report_error) {
        super(_transfer_fn, _storm_conf, _puller, topology_context,
                _user_context, _task_stats, taskStatus, _report_error);
        
        this.spout = _spout;
        
        ackerQueue = new LinkedBlockingQueue<Runnable>();
        // sending Tuple's TimeCacheMap
        this.pending = new TimeCacheMap(message_timeout_secs,
                Acker.TIMEOUT_BUCKET_NUM,
                new SpoutTimeCallBack<Object, Object>(ackerQueue, spout,
                        storm_conf, task_stats));
        
        this.max_spout_pending = JStormUtils.parseInt(storm_conf
                .get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
        if (max_spout_pending != null && max_spout_pending.intValue() == 1) {
            LOG.info("Do recv/ack in execute thread");
            supportRecvThread = false;
        } else {
            LOG.info("Do recv/ack in extra thread");
            supportRecvThread = true;
        }
        
        if (supportRecvThread == true) {
            Thread   ackerThread = new Thread(new AckerRunnable());
            ackerThread.setName("SpoutAckThread");
            ackerThread.setDaemon(true);
            ackerThread.setPriority(Thread.MAX_PRIORITY);
            ackerThread.start();
        }
        
        Thread recvThread = new Thread(new AckerRecvRunnable());
        recvThread.setName("SpoutRecvThread");
        recvThread.setDaemon(true);
        recvThread.setPriority(Thread.MAX_PRIORITY);
        recvThread.start();
        
        // collector, in fact it call send_spout_msg
        this.output_collector = new SpoutCollector(taskId, spout, task_stats,
                sendTargets, storm_conf, _transfer_fn, pending,
                topology_context, ackerQueue, _report_error);
        
        try {
            this.spout.open(storm_conf, userTopologyCtx,
                    new SpoutOutputCollector(output_collector));
        } catch (Exception e) {
            error = e;
            LOG.error("spout open error ", e);
            report_error.report(e);
        }
        
        LOG.info("Successfully create SpoutExecutors " + idStr);
    }
    
    @Override
    public void run() {
        if (firstTime == true) {
            int delayRun = JStormUtils.parseInt(
                    storm_conf.get(ConfigExtension.SPOUT_DELAY_RUN), 60);
            
            // wait other bolt is ready
            JStormUtils.sleepMs(delayRun * 1000);
            
            firstTime = false;
            
            LOG.info(idStr + " is ready ");
        }
        
        if (supportRecvThread == false) {
            executeEvent();
        }
        
        if (taskStatus.isRun() == false) {
            if (supportRecvThread == true) {
                JStormUtils.sleepMs(10);
            }else {
                JStormUtils.sleepMs(1);
            }
            return;
        }
        
        // if don't need ack, pending map will be always empty
        if (max_spout_pending == null || pending.size() < max_spout_pending) {
            
            try {
                
                spout.nextTuple();
            } catch (Exception e) {
                error = e;
                LOG.error("spout execute error ", e);
                report_error.report(e);
            }
            
            return;
        } else {
            if (supportRecvThread == true) {
                JStormUtils.sleepMs(1);
            }else {
                // if here do sleep, the tps will slow down much
                //JStormUtils.sleepNs(100);
                
            }
        }
        
    }
    
    private void executeEvent() {
        try {
            
            while (true) {
                // evnt is ack or fail
                Runnable event = null;
                if (supportRecvThread == true) {
                    // it is blocking
                    try {
                        event = ackerQueue.take();
                    } catch (InterruptedException e) {
                    }
                } else {
                    // it is non-blocking
                    event = ackerQueue.poll();
                }
                if (event == null) {
                    return;
                }
                
                event.run();
            }
        }catch (Exception e) {
            LOG.info("Unknow excpetion ", e);
            report_error.report(e);
        }
    }
    
    private void pushQueue() {
        //        Tuple tuple = recv_tuple_queue.poll();
        Tuple tuple = recv();
        if (tuple == null) {
            return;
        }
        
        Object id = tuple.getValue(0);
        Object obj = pending.remove(id);
        
        if (obj == null) {
            LOG.warn("Pending map no entry:" + id + ", pending size:" + pending.size());
            return;
        }
        
        TupleInfo tupleInfo = (TupleInfo) obj;
        
        String stream_id = tuple.getSourceStreamId();
        if (stream_id.equals(Acker.ACKER_ACK_STREAM_ID)) {
            
            ackerQueue.offer(new AckSpoutMsg(spout, tupleInfo.getMessageId(),
                    isDebug, tupleInfo.getStream(), tupleInfo.getTimestamp(),
                    task_stats));
        } else if (stream_id.equals(Acker.ACKER_FAIL_STREAM_ID)) {
            Long time_delta = null;
            
            ackerQueue.offer(new FailSpoutMsg(spout, tupleInfo, task_stats,
                    isDebug));
        }
        
        
    }
    
    class AckerRunnable implements Runnable {
        
        @Override
        public void run() {
            while (SpoutExecutors.this.taskStatus.isShutdown() == false) {
                executeEvent();
                
            }
            
            LOG.info("Successfully shutdown Spout's acker thread " + idStr);
        }
        
    }
    
    class AckerRecvRunnable implements Runnable {
        
        @Override
        public void run() {
            while (SpoutExecutors.this.taskStatus.isShutdown() == false) {
                
                try {
                    pushQueue();
                } catch (Exception e) {
                    LOG.error("Failed to handle recv event ", e);
                    report_error.report(e);
                }
                
            }
            
            LOG.info("Successfully shutdown Spout's acker thread " + idStr);
        }
        
    }
    
}
