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
package com.alibaba.jstorm.task.error;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.StormClusterState;

/**
 * Task error report callback
 * 
 * @author yannian
 * 
 */
public class TaskReportError implements ITaskReportErr {
    private static Logger LOG = LoggerFactory.getLogger(TaskReportError.class);
    private StormClusterState zkCluster;
    private String topology_id;
    private int task_id;

    public TaskReportError(StormClusterState _storm_cluster_state,
            String _topology_id, int _task_id) {
        this.zkCluster = _storm_cluster_state;
        this.topology_id = _topology_id;
        this.task_id = _task_id;
    }

    @Override
    public void report(Throwable error) {

        LOG.error("Report error to /ZK/taskerrors/" + topology_id + "/"
                + task_id + "\n", error);
        try {
            zkCluster.report_task_error(topology_id, task_id, error);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("Failed update error to /ZK/taskerrors/" + topology_id
                    + "/" + task_id + "\n", e);
        }

    }

}
