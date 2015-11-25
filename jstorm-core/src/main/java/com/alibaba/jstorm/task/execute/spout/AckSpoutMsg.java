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
package com.alibaba.jstorm.task.execute.spout;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.ISpout;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;

import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * The action after spout receive one ack tuple
 * 
 * @author yannian/Longda
 * 
 */
public class AckSpoutMsg implements IAckMsg {
    private static Logger LOG = LoggerFactory.getLogger(AckSpoutMsg.class);

    private ISpout spout;
    private Tuple tuple;
    private TupleInfo tupleInfo;
    private Object msgId;
    private String stream;
    private List<Object> values;
    private TaskBaseMetric task_stats;
    private boolean isDebug = false;

    public AckSpoutMsg(ISpout _spout, Tuple tuple, TupleInfo tupleInfo, TaskBaseMetric _task_stats, boolean _isDebug) {

        this.task_stats = _task_stats;

        this.spout = _spout;
        this.isDebug = _isDebug;

        this.msgId = tupleInfo.getMessageId();
        this.stream = tupleInfo.getStream();
        
        this.values = tupleInfo.getValues();

        this.tuple = tuple;
        this.tupleInfo = tupleInfo;
    }

    public void run() {
        if (isDebug) {
            LOG.info("Acking message {}", msgId);
        }

        if (spout instanceof IAckValueSpout) {
            IAckValueSpout ackValueSpout = (IAckValueSpout) spout;
            ackValueSpout.ack(msgId, values);
        } else {
            spout.ack(msgId);
        }

        long latency = 0, lifeCycle = 0;
        if (tupleInfo.getTimestamp() != 0) {
        	long endTime = System.nanoTime();
        	latency = (endTime - tupleInfo.getTimestamp())/TimeUtils.NS_PER_US;
        	if (tuple != null && tuple instanceof TupleExt) {
        		lifeCycle = (System.currentTimeMillis() - ((TupleExt) tuple).getCreationTimeStamp()) * TimeUtils.NS_PER_US;
        	}
        }
        task_stats.spout_acked_tuple(stream, latency, lifeCycle);
    }

}
