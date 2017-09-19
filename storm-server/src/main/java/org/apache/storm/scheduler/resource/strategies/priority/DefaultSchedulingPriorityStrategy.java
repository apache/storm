/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource.strategies.priority;

import java.util.Map;

import org.apache.storm.scheduler.ISchedulingState;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSchedulingPriorityStrategy implements ISchedulingPriorityStrategy {
    private static final Logger LOG = LoggerFactory
            .getLogger(DefaultSchedulingPriorityStrategy.class);

    @Override
    public TopologyDetails getNextTopologyToSchedule(ISchedulingState schedulingState, Map<String, User> userMap) {
        User nextUser = getNextUser(schedulingState, userMap);
        if (nextUser == null) {
            return null;
        }
        return nextUser.getNextTopologyToSchedule(schedulingState);
    }

    public User getNextUser(ISchedulingState cluster, Map<String, User> userMap) {
        double least = Double.POSITIVE_INFINITY;
        User ret = null;
        final double totalCpu = cluster.getClusterTotalCpuResource();
        final double totalMem = cluster.getClusterTotalMemoryResource();
        for (User user : userMap.values()) {
            if (user.hasTopologyNeedSchedule(cluster)) {
                double userResourcePoolAverageUtilization = user.getResourcePoolAverageUtilization(cluster);
                if (least > userResourcePoolAverageUtilization) {
                    ret = user;
                    least = userResourcePoolAverageUtilization;
                } else if (Math.abs(least - userResourcePoolAverageUtilization) < 0.0001) {
                    // if ResourcePoolAverageUtilization is equal to the user that is being compared
                    double currentCpuPercentage = ret.getCpuResourceGuaranteed() / totalCpu;
                    double currentMemoryPercentage = ret.getMemoryResourceGuaranteed() / totalMem;
                    double currentAvgPercentage = (currentCpuPercentage + currentMemoryPercentage) / 2.0;

                    double userCpuPercentage = user.getCpuResourceGuaranteed() / totalCpu;
                    double userMemoryPercentage = user.getMemoryResourceGuaranteed() / totalMem;
                    double userAvgPercentage = (userCpuPercentage + userMemoryPercentage) / 2.0;
                    if (userAvgPercentage > currentAvgPercentage) {
                        ret = user;
                        least = userResourcePoolAverageUtilization;
                    }
                }
            }
        }
        return ret;
    }
}
