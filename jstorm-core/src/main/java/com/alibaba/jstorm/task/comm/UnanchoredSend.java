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
package com.alibaba.jstorm.task.comm;

import java.util.List;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.TupleImplExt;

import com.alibaba.jstorm.task.TaskTransfer;

/**
 * Send init/ack/fail tuple to acker
 * 
 * @author yannian
 * 
 */

public class UnanchoredSend {
    public static void send(TopologyContext topologyContext,
            TaskSendTargets taskTargets, TaskTransfer transfer_fn,
            String stream, List<Object> values) {

        java.util.List<Integer> tasks = taskTargets.get(stream, values);
        if (tasks.size() == 0) {
            return;
        }

        Integer taskId = topologyContext.getThisTaskId();

        for (Integer task : tasks) {
            TupleImplExt tup =
                    new TupleImplExt(topologyContext, values, taskId, stream);
            tup.setTargetTaskId(task);

            transfer_fn.transfer(tup);
        }
    }
}
