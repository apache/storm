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

package org.apache.storm.command;

import java.util.List;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.utils.NimbusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListTopologies {
    private static final Logger LOG = LoggerFactory.getLogger(ListTopologies.class);
    private static final String MSG_FORMAT = "%-20s %-10s %-10s %-12s %-12s %-20s %-20s\n";

    public static void main(String[] args) throws Exception {
        NimbusClient.withConfiguredClient(nimbus -> {
            List<TopologySummary> topologies = nimbus.getTopologySummaries();
            if (topologies == null || topologies.isEmpty()) {
                System.out.println("No topologies running.");
            } else {
                System.out.printf(MSG_FORMAT,
                        "Topology_name",
                        "Status",
                        "Num_tasks",
                        "Num_workers",
                        "Uptime_secs",
                        "Topology_Id",
                        "Owner");
                System.out.println("----------------------------------------------------------------------------------------");
                for (TopologySummary topology : topologies) {
                    System.out.printf(MSG_FORMAT, topology.get_name(), topology.get_status(),
                                      topology.get_num_tasks(), topology.get_num_workers(),
                                      topology.get_uptime_secs(), topology.get_id(),
                                      topology.get_owner());
                }
            }
        });
    }
}
