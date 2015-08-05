/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.daemon.worker;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.WorkerSlot;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * Shutdown worker
 * 
 * @author yannian/Longda
 * 
 */
public class WorkerShutdown implements ShutdownableDameon {
    private static Logger LOG = LoggerFactory.getLogger(WorkerShutdown.class);

    public static final String HOOK_SIGNAL = "USR2";

    private List<TaskShutdownDameon> shutdowntasks;
    private AtomicBoolean shutdown;
    private ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;
    private IContext context;
    private List<AsyncLoopThread> threads;
    private StormClusterState zkCluster;
    private ClusterState cluster_state;
    private ScheduledExecutorService threadPool;
    private IConnection recvConnection;

    // active nodeportSocket context zkCluster zkClusterstate
    public WorkerShutdown(WorkerData workerData, List<AsyncLoopThread> _threads) {

        this.shutdowntasks = workerData.getShutdownTasks();
        this.threads = _threads;

        this.shutdown = workerData.getShutdown();
        this.nodeportSocket = workerData.getNodeportSocket();
        this.context = workerData.getContext();
        this.zkCluster = workerData.getZkCluster();
        this.cluster_state = workerData.getZkClusterstate();
        this.threadPool = workerData.getThreadPool();
        this.recvConnection = workerData.getRecvConnection();

        Runtime.getRuntime().addShutdownHook(new Thread(this));

        // PreCleanupTasks preCleanupTasks = new PreCleanupTasks();
        // // install signals
        // Signal sig = new Signal(HOOK_SIGNAL);
        // Signal.handle(sig, preCleanupTasks);
    }

    @Override
    public void shutdown() {

        if (shutdown.getAndSet(true) == true) {
            LOG.info("Worker has been shutdown already");
            return;
        }
        
        if(recvConnection != null) {
        	recvConnection.close();
        }

        AsyncLoopRunnable.getShutdown().set(true);
        threadPool.shutdown();
        

        // shutdown tasks
        for (ShutdownableDameon task : shutdowntasks) {
            task.shutdown();
        }

        // shutdown worker's demon thread
        // refreshconn, refreshzk, hb, drainer
        for (AsyncLoopThread t : threads) {
            LOG.info("Begin to shutdown " + t.getThread().getName());
            t.cleanup();
            JStormUtils.sleepMs(100);
            t.interrupt();
            // try {
            // t.join();
            // } catch (InterruptedException e) {
            // LOG.error("join thread", e);
            // }
            LOG.info("Successfully " + t.getThread().getName());
        }

        // send data to close connection
        for (WorkerSlot k : nodeportSocket.keySet()) {
            IConnection value = nodeportSocket.get(k);
            value.close();
        }

        context.term();

        // close ZK client
        try {
            zkCluster.disconnect();
            cluster_state.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.info("Shutdown error,", e);
        }

        JStormUtils.halt_process(0, "!!!Shutdown!!!");
    }

    public void join() throws InterruptedException {
        for (TaskShutdownDameon task : shutdowntasks) {
            task.join();
        }
        for (AsyncLoopThread t : threads) {
            t.join();
        }

    }

    public boolean waiting() {
        Boolean isExistsWait = false;
        for (ShutdownableDameon task : shutdowntasks) {
            if (task.waiting()) {
                isExistsWait = true;
                break;
            }
        }
        for (AsyncLoopThread thr : threads) {
            if (thr.isSleeping()) {
                isExistsWait = true;
                break;
            }
        }
        return isExistsWait;
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        shutdown();
    }

    // class PreCleanupTasks implements SignalHandler {
    //
    // @Override
    // public void handle(Signal arg0) {
    // LOG.info("Receive " + arg0.getName() + ", begin to do pre_cleanup job");
    //
    // for (ShutdownableDameon task : shutdowntasks) {
    // task.shutdown();
    // }
    //
    // LOG.info("Successfully do pre_cleanup job");
    // }
    //
    // }

}
