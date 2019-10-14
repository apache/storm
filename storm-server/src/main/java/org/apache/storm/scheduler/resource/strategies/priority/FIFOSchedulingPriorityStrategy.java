/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler.resource.strategies.priority;

import java.util.Collections;
import java.util.Comparator;
import org.apache.storm.scheduler.ISchedulingState;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class FIFOSchedulingPriorityStrategy extends DefaultSchedulingPriorityStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(FIFOSchedulingPriorityStrategy.class);

    @Override
    protected SimulatedUser getSimulatedUserFor(User u, ISchedulingState cluster) {
        return new FIFOSimulatedUser(u, cluster);
    }

    protected static class FIFOSimulatedUser extends SimulatedUser {

        public FIFOSimulatedUser(User other, ISchedulingState cluster) {
            super(other, cluster);
        }

        @Override
        public double getScore(double availableCpu, double availableMemory) {
            TopologyDetails td = getNextHighest();
            double origScore = getScore(availableCpu, availableMemory, td);
            if (origScore < 0) {
                return origScore;
            }
            //Not enough guaranteed use the age of the topology instead.
            //TODO need a good way to only do this once...
            Collections.sort(tds, new TopologyBySubmissionTimeComparator());
            td = getNextHighest();
            if (td != null) {
                LOG.debug("SCORE FOR {} is {}", td.getId(), td.getUpTime());
                return td.getUpTime();
            }
            return Double.MAX_VALUE;
        }
    }

    /**
     * Comparator that sorts topologies by submission time.
     */
    private static class TopologyBySubmissionTimeComparator implements Comparator<TopologyDetails> {

        @Override
        public int compare(TopologyDetails topo1, TopologyDetails topo2) {
            if (topo1.getUpTime() > topo2.getUpTime()) {
                return 1;
            } else if (topo1.getUpTime() < topo2.getUpTime()) {
                return -1;
            } else {
                return topo1.getId().compareTo(topo2.getId());
            }
        }
    }
}
