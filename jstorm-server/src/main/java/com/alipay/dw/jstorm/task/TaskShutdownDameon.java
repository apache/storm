package com.alipay.dw.jstorm.task;

import org.apache.log4j.Logger;

import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;

import com.alipay.dw.jstorm.callback.AsyncLoopThread;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.zeroMq.IRecvConnection;
import com.alipay.dw.jstorm.zeroMq.MQContext;

/**
 * shutdown one task
 * 
 * @author yannian/Longda
 * 
 */
public class TaskShutdownDameon implements ShutdownableDameon {
    private static Logger     LOG      = Logger.getLogger(TaskShutdownDameon.class);
    
    public static final byte  QUIT_MSG = (byte) 0xff;
    
    private TaskStatus        taskStatus;
    private String            storm_id;
    private Integer           task_id;
    private MQContext         mq_context;
    private AsyncLoopThread[] all_threads;
    private StormClusterState zkCluster;
    private IRecvConnection   puller;
    private Object            task_obj;
    private AsyncLoopThread   heartbeat_thread;
    
    public TaskShutdownDameon(TaskStatus taskStatus, String storm_id,
            Integer task_id, MQContext mq_context,
            AsyncLoopThread[] all_threads, StormClusterState zkCluster,
            IRecvConnection puller, Object task_obj,
            AsyncLoopThread heartbeat_thread) {
        this.taskStatus = taskStatus;
        this.storm_id = storm_id;
        this.task_id = task_id;
        this.mq_context = mq_context;
        this.all_threads = all_threads;
        this.zkCluster = zkCluster;
        this.puller = puller;
        this.task_obj = task_obj;
        this.heartbeat_thread = heartbeat_thread;
        
    }
    
    @Override
    public void shutdown() {
        LOG.info("Begin to shut down task " + storm_id + ":" + task_id);
        
        // all thread will check the taskStatus
        // once it has been set SHUTDOWN, it will quit
        taskStatus.setStatus(TaskStatus.SHUTDOWN);
        
        // send command to task recv thread
        // in fact, no use
        // mq_context.send_local_task_empty(storm_id, task_id);
        
        for (AsyncLoopThread thr : all_threads) {
            thr.interrupt();
            try {
                thr.join();
            } catch (InterruptedException e) {
            }
        }
        try {
            zkCluster.remove_task_heartbeat(storm_id, task_id);
            zkCluster.disconnect();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.info(e);
        }
        
        
        closeComponent(task_obj);
        
        LOG.info("Successfully shut down task " + storm_id + ":" + task_id);
        
    }
    
    public void join() throws InterruptedException {
        for (AsyncLoopThread t : all_threads) {
            t.join();
        }
    }
    
    private void closeComponent(Object _task_obj) {
        if (_task_obj instanceof IBolt) {
            ((IBolt) _task_obj).cleanup();
        }
        
        if (_task_obj instanceof ISpout) {
            ((ISpout) _task_obj).close();
        }
    }
    
    @Override
    public boolean waiting() {
        return heartbeat_thread.isSleeping();
    }
    
    public void deactive() {
        
        if (task_obj instanceof ISpout) {
            taskStatus.setStatus(TaskStatus.PAUSE);
            ((ISpout) task_obj).deactivate();
        }
        
    }
    
    public void active() {
        if (task_obj instanceof ISpout) {
            taskStatus.setStatus(TaskStatus.RUN);
            ((ISpout) task_obj).activate();
        }
    }
    
    @Override
    public void run() {
        // TODO Auto-generated method stub
        shutdown();
    }
}
