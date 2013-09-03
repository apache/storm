package com.alipay.dw.jstorm.task.execute;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.IBolt;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;

import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.daemon.worker.WorkerTransfer;
import com.alipay.dw.jstorm.stats.CommonStatsRolling;
import com.alipay.dw.jstorm.task.TaskStatus;
import com.alipay.dw.jstorm.task.acker.Acker;
import com.alipay.dw.jstorm.task.comm.TaskSendTargets;
import com.alipay.dw.jstorm.task.error.ITaskReportErr;
import com.alipay.dw.jstorm.utils.RunCounter;
import com.alipay.dw.jstorm.utils.TimeCacheQueue;
import com.alipay.dw.jstorm.zeroMq.IRecvConnection;

/**
 * 
 * BoltExecutor
 * 
 * @author yannian/Longda
 * 
 */
public class BoltExecutors extends BaseExecutors {
    private static Logger                LOG            = Logger.getLogger(BoltExecutors.class);
    
    protected IBolt                      bolt;
    
    protected TimeCacheMap<Tuple, Long>  tuple_start_times;
    
    // internal outputCollector is BoltCollector
    private OutputCollector              outputCollector;
    
    // don't use TimeoutQueue for recv_tuple_queue, 
    // then other place should check the queue size
    //protected Thread                       recvThread;
    //protected TimeCacheQueue<Tuple> recv_tuple_queue;
    // don't take care of competition, one thread offer/one thread poll
    protected LinkedBlockingQueue<Tuple> recv_tuple_queue;
    
    
    public BoltExecutors(IBolt _bolt, WorkerTransfer _transfer_fn,
            Map storm_conf, IRecvConnection _puller, TaskSendTargets _send_fn,
            TaskStatus taskStatus, TopologyContext sysTopologyCxt,
            TopologyContext userTopologyCxt, CommonStatsRolling _task_stats,
            ITaskReportErr _report_error) {
        
        super(_transfer_fn, storm_conf, _puller, sysTopologyCxt,
                userTopologyCxt, _task_stats, taskStatus, _report_error);
        
        this.bolt = _bolt;
        
        // create TimeCacheMap
        
        this.tuple_start_times = new TimeCacheMap<Tuple, Long>(
                message_timeout_secs, Acker.TIMEOUT_BUCKET_NUM);
        
        TimeCacheQueue.DefaultExpiredCallback<Tuple> logExpireCb = new TimeCacheQueue.DefaultExpiredCallback<Tuple>(
                idStr);
        //      this.recv_tuple_queue = new TimeCacheQueue<Tuple>(message_timeout_secs,
        //      TimeCacheQueue.DEFAULT_NUM_BUCKETS, logExpireCb);
        this.recv_tuple_queue = new LinkedBlockingQueue<Tuple>();
        
        Thread recvThread = new Thread(new RecvRunnable());
        recvThread.setName("BoltRecvThread");
        recvThread.setPriority(Thread.MAX_PRIORITY);
        recvThread.setDaemon(true);
        recvThread.start();

        // create BoltCollector
        IOutputCollector output_collector = new BoltCollector(
                message_timeout_secs, _report_error, _send_fn, storm_conf,
                _transfer_fn, sysTopologyCxt, taskId, tuple_start_times,
                _task_stats);
        
        outputCollector = new OutputCollector(output_collector);
        
        try {
            //do prepare
            bolt.prepare(storm_conf, userTopologyCxt, outputCollector);
        } catch (Exception e) {
            error = e;
            LOG.error("bolt prepare error ", e);
            report_error.report(e);
        }
        
        LOG.info("Successfully create BoltExecutors " + idStr);
        
    }
    
    @Override
    public void run() {
        
        while (true) {
            
            Tuple tuple = null;
            try {
                tuple = recv_tuple_queue.take();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                return;
            }
            if (tuple == null) {
                return;
            }
            
            tuple_start_times.put(tuple, System.currentTimeMillis());
            
            try {
                bolt.execute(tuple);
            } catch (Exception e) {
                error = e;
                LOG.error("bolt execute error ", e);
                report_error.report(e);
            }
        }
        
    }
    
    class RecvRunnable implements Runnable {
        
        @Override
        public void run() {
            
            while (taskStatus.isShutdown() == false) {
                Tuple tuple = BoltExecutors.this.recv();
                if (tuple == null) {
                    continue;
                }
                recv_tuple_queue.offer(tuple);
            }
            
            LOG.info("Successfully shutdown recv thread of " + idStr);
            
        }
        
    }
    
}
