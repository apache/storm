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
package com.alibaba.jstorm.task.group;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.DisruptorQueue;

import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.RandomRange;

/**
 * 
 * 
 * @author zhongyan.feng
 * @version
 */
public class MkLocalFirst extends Shuffer {
    private static final Logger LOG = LoggerFactory
            .getLogger(MkLocalFirst.class);

    private List<Integer> allOutTasks = new ArrayList<Integer>();
    private List<Integer> localOutTasks = new ArrayList<Integer>();
    private List<Integer> remoteOutTasks = new ArrayList<Integer>();
    private RandomRange randomrange;
    private RandomRange remoteRandomRange;
    private boolean isLocalWorkerAvail;
    private WorkerData workerData;
    private IntervalCheck intervalCheck;

    public MkLocalFirst(List<Integer> workerTasks, List<Integer> allOutTasks,
            WorkerData workerData) {
        super(workerData);

        intervalCheck = new IntervalCheck();
        intervalCheck.setInterval(10);

        this.allOutTasks.addAll(allOutTasks);
        this.workerData = workerData;

        List<Integer> localWorkerOutTasks = new ArrayList<Integer>();

        for (Integer outTask : allOutTasks) {
            if (workerTasks.contains(outTask)) {
                localWorkerOutTasks.add(outTask);
            }
        }

        remoteOutTasks.addAll(allOutTasks);
        if (localWorkerOutTasks.size() != 0) {
            isLocalWorkerAvail = true;
            localOutTasks.addAll(localWorkerOutTasks);
            remoteOutTasks.removeAll(localWorkerOutTasks);
        } else {
            isLocalWorkerAvail = false;
        }
        randomrange = new RandomRange(localOutTasks.size());
        remoteRandomRange = new RandomRange(remoteOutTasks.size());

        LOG.info("Local out tasks:" + localOutTasks + ", Remote out tasks:" + remoteOutTasks);
    }

    @Override
    protected int getActiveTask(RandomRange randomrange, List<Integer> outTasks) {
        int index = randomrange.nextInt();
        int size = outTasks.size();
        int i = 0;

        for (i = 0; i < size; i++) {
            Integer taskId = outTasks.get(index);
            boolean taskStatus = workerData.isOutboundTaskActive(taskId);
            DisruptorQueue exeQueue =
                    (workerData.getInnerTaskTransfer().get(taskId));
            float queueLoadRatio = exeQueue != null ? exeQueue.pctFull() : 0;
            if (taskStatus && queueLoadRatio < 1.0)
                break;
            else
                index = randomrange.nextInt();
        }

        return (i < size ? index : -1);
    }

    private List<Integer> intraGroup(List<Object> values) {
        if (localOutTasks.size() == 0)
            return null;

        int index = getActiveTask(randomrange, localOutTasks);
        if (index == -1) {
            return null;
        }
        return JStormUtils.mk_list(localOutTasks.get(index));
    }

    private List<Integer> interGroup(List<Object> values) {
        int index = getActiveTask(remoteRandomRange, remoteOutTasks);
        if (index == -1) {
            index = randomrange.nextInt();
        }
        return JStormUtils.mk_list(remoteOutTasks.get(index));
    }
    

    public List<Integer> grouper(List<Object> values) {
        List<Integer> ret;
        ret = intraGroup(values);
        if (ret == null)
            ret = interGroup(values);
        return ret;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
