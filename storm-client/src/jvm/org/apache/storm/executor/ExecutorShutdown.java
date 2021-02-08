/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.executor;

import java.util.ArrayList;
import java.util.List;
import org.apache.storm.Constants;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.Task;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.spout.ISpout;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorShutdown implements Shutdownable, IRunningExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorShutdown.class);

    private final Executor executor;
    private final List<Utils.SmartThread> threads;
    private final ArrayList<Task> taskDatas;
    private final JCQueue receiveQueue;

    public ExecutorShutdown(Executor executor, List<Utils.SmartThread> threads, ArrayList<Task> taskDatas, JCQueue recvQueue) {
        this.executor = executor;
        this.threads = threads;
        this.taskDatas = taskDatas;
        this.receiveQueue = recvQueue;
    }

    @Override
    public ExecutorStats renderStats() {
        return executor.getStats().renderStats();
    }

    @Override
    public List<Long> getExecutorId() {
        return executor.getExecutorId();
    }

    @Override
    public void credentialsChanged(Credentials credentials) {
        executor.setNeedToRefreshCreds();
    }

    @Override
    public void loadChanged(LoadMapping loadMapping) {
        executor.reflectNewLoadMapping(loadMapping);
    }

    @Override
    public JCQueue getReceiveQueue() {
        return receiveQueue;
    }

    @Override
    public boolean publishFlushTuple() {
        return executor.publishFlushTuple();
    }

    @Override
    public void shutdown() {
        try {
            LOG.info("Shutting down executor " + executor.getComponentId() + ":" + executor.getExecutorId());
            executor.getReceiveQueue().close();
            for (Utils.SmartThread t : threads) {
                t.interrupt();
            }
            for (Utils.SmartThread t : threads) {
                LOG.debug("Executor " + executor.getComponentId() + ":" + executor.getExecutorId() + " joining thread " + t.getName());
                //Don't wait forever.
                //This is to avoid the deadlock between the executor thread (t) and the shutdown hook (which invokes Worker::shutdown)
                //when it is the executor thread (t) who invokes the shutdown hook. See STORM-3658.
                long waitMs = 100;
                t.join(waitMs);
                if (t.isAlive()) {
                    LOG.warn("Thread {} is still alive ({} ms after interruption). Stop waiting for it.", t.getName(), waitMs);
                }
            }
            executor.getStats().cleanupStats();
            for (Task task : taskDatas) {
                if (task == null) {
                    continue;
                }
                TopologyContext userContext = task.getUserContext();
                for (ITaskHook hook : userContext.getHooks()) {
                    hook.cleanup();
                }
            }
            executor.getStormClusterState().disconnect();
            if (executor.getOpenOrPrepareWasCalled().get()) {
                for (Task task : taskDatas) {
                    if (task == null) {
                        continue;
                    }
                    Object object = task.getTaskObject();
                    if (object instanceof ISpout) {
                        ((ISpout) object).close();
                    } else if (object instanceof IBolt) {
                        ((IBolt) object).cleanup();
                    } else {
                        LOG.error("unknown component object");
                    }
                }
            }
            LOG.info("Shut down executor " + executor.getComponentId() + ":" + executor.getExecutorId());
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }
}
