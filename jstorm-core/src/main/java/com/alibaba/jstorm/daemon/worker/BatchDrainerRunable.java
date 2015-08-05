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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.utils.DisruptorRunable;
import com.alibaba.jstorm.utils.Pair;

//import com.alibaba.jstorm.message.zeroMq.ISendConnection;

/**
 * 
 * Tuple sender
 * 
 * @author yannian
 * 
 */
public class BatchDrainerRunable extends DisruptorRunable {
    private final static Logger LOG = LoggerFactory
            .getLogger(BatchDrainerRunable.class);

    public BatchDrainerRunable(WorkerData workerData) {
        super(workerData.getSendingQueue(), MetricDef.BATCH_DRAINER_THREAD);

    }

    @Override
    public void handleEvent(Object event, boolean endOfBatch) throws Exception {

        Pair<IConnection, List<TaskMessage>> pair =
                (Pair<IConnection, List<TaskMessage>>) event;

        pair.getFirst().send(pair.getSecond());

    }

}
