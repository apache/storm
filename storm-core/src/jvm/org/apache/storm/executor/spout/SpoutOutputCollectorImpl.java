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
package org.apache.storm.executor.spout;

import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.Task;
import org.apache.storm.executor.ExecutorCommon;
import org.apache.storm.executor.ExecutorData;
import org.apache.storm.executor.TupleInfo;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MutableLong;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SpoutOutputCollectorImpl implements ISpoutOutputCollector {

    private final ExecutorData executorData;
    private final Task taskData;
    private final int taskId;
    private final MutableLong emittedCount;
    private final boolean isAcker;
    private final Random random;
    private final Boolean isEventLoggers;
    private final Boolean isDebug;
    private final RotatingMap<Long, TupleInfo> pending;

    public SpoutOutputCollectorImpl(ISpout spout, ExecutorData executorData, Task taskData, int taskId, MutableLong emittedCount, boolean isAcker,
            Random random, Boolean isEventLoggers, Boolean isDebug, RotatingMap<Long, TupleInfo> pending) {
        this.executorData = executorData;
        this.taskData = taskData;
        this.taskId = taskId;
        this.emittedCount = emittedCount;
        this.isAcker = isAcker;
        this.random = random;
        this.isEventLoggers = isEventLoggers;
        this.isDebug = isDebug;
        this.pending = pending;
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        return sendSpoutMsg(streamId, tuple, messageId, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        sendSpoutMsg(streamId, tuple, messageId, taskId);
    }

    @Override
    public long getPendingCount() {
        return pending.size();
    }

    @Override
    public void reportError(Throwable error) {
        executorData.getReportError().report(error);
    }

    private List<Integer> sendSpoutMsg(String stream, List<Object> values, Object messageId, Integer outTaskId) {
        emittedCount.increment();

        java.util.List<Integer> outTasks;
        if (outTaskId != null) {
            outTasks = taskData.getOutgoingTasks(outTaskId, stream, values);
        } else {
            outTasks = taskData.getOutgoingTasks(stream, values);
        }
        if (outTasks.size() == 0) {
            // don't need send tuple to other task
            return outTasks;
        }
        List<Long> ackSeq = new ArrayList<Long>();
        boolean needAck = (messageId != null) && isAcker;

        long rootId = MessageId.generateId(random);
        for (Integer t : outTasks) {
            MessageId msgId;
            if (needAck) {
                long as = MessageId.generateId(random);
                msgId = MessageId.makeRootId(rootId, as);
                ackSeq.add(as);
            } else {
                msgId = MessageId.makeUnanchored();
            }

            TupleImpl tuple = new TupleImpl(executorData.getWorkerTopologyContext(), values, t, stream, msgId);
            executorData.getExecutorTransfer().transfer(t, tuple);
        }
        if (isEventLoggers) {
            ExecutorCommon.sendToEventLogger(executorData, taskData, values, executorData.getComponentId(), messageId, random);
        }

        if (needAck) {
            TupleInfo info = new TupleInfo();
            info.setTaskId(taskId);
            info.setStream(stream);
            info.setMessageId(messageId);
            if (isDebug) {
                info.setValues(values);
            }
            try {
                if (executorData.getSampler().call())
                    info.setTimestamp(System.currentTimeMillis());
            } catch (Exception e) {
                throw Utils.wrapInRuntime(e);
            }

            pending.put(rootId, info);
            List<Object> ackerTuple = new Values(rootId, Utils.bitXorVals(ackSeq), taskId);
            ExecutorCommon.sendUnanchored(taskData, Acker.ACKER_INIT_STREAM_ID, ackerTuple, executorData.getExecutorTransfer());
        } else if (messageId != null) {
            TupleInfo info = new TupleInfo();
            info.setStream(stream);
            info.setValues(values);
            info.setMessageId(messageId);
            info.setTimestamp(0);
            info.setId("0:");
            ExecutorCommon.ackSpoutMsg(executorData, taskData, info);
        }

        return outTasks;
    }
}
