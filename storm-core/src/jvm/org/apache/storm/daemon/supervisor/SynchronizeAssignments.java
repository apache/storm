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
package org.apache.storm.daemon.supervisor;

import org.apache.storm.generated.SupervisorAssignments;

/**
 * A runnable which will synchronize assignments to node local and then worker processes.
 */
public class SynchronizeAssignments implements Runnable{
    private Supervisor supervisor;
    private SupervisorAssignments assignments;
    private ReadClusterState readClusterState;

    public SynchronizeAssignments(Supervisor supervisor, SupervisorAssignments assignments, ReadClusterState readClusterState) {
        this.supervisor = supervisor;
        this.assignments = assignments;
        this.readClusterState = readClusterState;
    }

    @Override
    public void run() {
        // first sync assignments to local, then sync processes.
        if (null == assignments) {
            supervisor.getAssignmentsFromMaster();
        } else {
            supervisor.assignedAssignmentsToLocal(assignments);
        }
        this.readClusterState.run();
    }
}
