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
package com.alibaba.jstorm.daemon.supervisor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.cluster.DaemonCommon;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.event.EventManager;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.PathUtils;

/**
 * supervisor shutdown manager which can shutdown supervisor
 */
public class SupervisorManger extends ShutdownWork implements SupervisorDaemon,
        DaemonCommon, Runnable {

    private static Logger LOG = LoggerFactory.getLogger(SupervisorManger.class);

    // private Supervisor supervisor;

    private Map conf;

    private String supervisorId;

    private AtomicBoolean shutdown;

    private Vector<AsyncLoopThread> threads;

    private EventManager processesEventManager;

    private EventManager eventManager;

    private Httpserver httpserver;

    private StormClusterState stormClusterState;

    private ConcurrentHashMap<String, String> workerThreadPidsAtom;

    private volatile boolean isFinishShutdown = false;

    public SupervisorManger(Map conf, String supervisorId,
            Vector<AsyncLoopThread> threads,
            EventManager processesEventManager, EventManager eventManager,
            Httpserver httpserver, StormClusterState stormClusterState,
            ConcurrentHashMap<String, String> workerThreadPidsAtom) {
        this.conf = conf;
        this.supervisorId = supervisorId;
        this.shutdown = new AtomicBoolean(false);
        this.threads = threads;
        this.processesEventManager = processesEventManager;
        this.eventManager = eventManager;
        this.httpserver = httpserver;
        this.stormClusterState = stormClusterState;
        this.workerThreadPidsAtom = workerThreadPidsAtom;

        Runtime.getRuntime().addShutdownHook(new Thread(this));
    }

    @Override
    public void shutdown() {
        if (shutdown.getAndSet(true) == true) {
            LOG.info("Supervisor has been shutdown before " + supervisorId);
            return;
        }
        LOG.info("Shutting down supervisor " + supervisorId);
        AsyncLoopRunnable.getShutdown().set(true);

        int size = threads.size();
        for (AsyncLoopThread thread : threads) {
            thread.cleanup();
            JStormUtils.sleepMs(10);
            thread.interrupt();
            // try {
            // thread.join();
            // } catch (InterruptedException e) {
            // LOG.error(e.getMessage(), e);
            // }
            LOG.info("Successfully shutdown thread:"
                    + thread.getThread().getName());
        }
        eventManager.shutdown();
        processesEventManager.shutdown();
        try {
            stormClusterState.disconnect();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("Failed to shutdown ZK client", e);
        }
        if (httpserver != null) {
            httpserver.shutdown();
        }

        // if (this.cgroupManager != null)
        // try {
        // this.cgroupManager.close();
        // } catch (IOException e) {
        // // TODO Auto-generated catch block
        // LOG.error("Fail to close cgroup", e);
        // }

        isFinishShutdown = true;

        JStormUtils.halt_process(0, "!!!Shutdown!!!");
    }

    @Override
    public void ShutdownAllWorkers() {
        LOG.info("Begin to shutdown all workers");
        String path;
        try {
            path = StormConfig.worker_root(conf);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            LOG.error("Failed to get Local worker dir", e1);
            return;
        }
        List<String> myWorkerIds = PathUtils.read_dir_contents(path);
        HashMap<String, String> workerId2topologyIds =
                new HashMap<String, String>();

        for (String workerId : myWorkerIds) {
            workerId2topologyIds.put(workerId, null);
        }

        shutWorker(conf, supervisorId, workerId2topologyIds,
                workerThreadPidsAtom, null, true, null, null);
    }

    @Override
    public Map getConf() {
        return conf;
    }

    @Override
    public String getId() {
        return supervisorId;
    }

    @Override
    public boolean waiting() {
        if (shutdown.get()) {
            return true;
        }

        Boolean bThread = true;
        int size = threads.size();
        for (int i = 0; i < size; i++) {
            if (!(Boolean) threads.elementAt(i).isSleeping()) {
                bThread = false;
                return false;
            }
        }
        boolean bManagers = true;
        if (eventManager.waiting() && processesEventManager.waiting()) {
            bManagers = false;
            return false;
        }
        return true;
    }

    public void run() {
        shutdown();
    }

    public boolean isFinishShutdown() {
        return isFinishShutdown;
    }
}
