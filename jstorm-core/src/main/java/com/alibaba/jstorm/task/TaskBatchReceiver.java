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
package com.alibaba.jstorm.task;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.task.TaskReceiver.DeserializeRunnable;
import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;

public class TaskBatchReceiver extends TaskReceiver {
    private static Logger LOG = LoggerFactory
            .getLogger(TaskBatchReceiver.class);

    public TaskBatchReceiver(Task task, int taskId, Map stormConf,
            TopologyContext topologyContext,
            Map<Integer, DisruptorQueue> innerTaskTransfer,
            TaskStatus taskStatus, String taskName) {
        super(task, taskId, stormConf, topologyContext, innerTaskTransfer,
                taskStatus, taskName);
    }

    @Override
    protected void setDeserializeThread() {
        this.deserializeThread =
                new AsyncLoopThread(new DeserializeBatchRunnable(
                        deserializeQueue, innerTaskTransfer.get(taskId)));
    }

    public class DeserializeBatchRunnable extends DeserializeRunnable {
        public DeserializeBatchRunnable(DisruptorQueue deserializeQueue,
                DisruptorQueue exeQueue) {
            super(deserializeQueue, exeQueue);
        }

        @Override
        protected Object deserialize(byte[] ser_msg) {
            long start = System.nanoTime();
            try {
                if (ser_msg == null) {
                    return null;
                }

                if (ser_msg.length == 0) {
                    return null;
                } else if (ser_msg.length == 1) {
                    byte newStatus = ser_msg[0];
                    LOG.info("Change task status as " + newStatus);
                    taskStatus.setStatus(newStatus);

                    return null;
                }

                // ser_msg.length > 1
                BatchTuple tuple = deserializer.deserializeBatch(ser_msg);
                if (isDebugRecv) {
                    LOG.info(idStr + " receive " + tuple.toString());
                }

                return tuple;
            } catch (Throwable e) {
                if (taskStatus.isShutdown() == false) {
                    LOG.error(
                            idStr + " recv thread error "
                                    + JStormUtils.toPrintableString(ser_msg)
                                    + "\n", e);
                }
            } finally {
                long end = System.nanoTime();
                deserializeTimer.update((end - start)/1000000.0d);
            }

            return null;
        }
    }
}