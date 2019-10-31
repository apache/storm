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

package org.apache.storm.sql.planner;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.storm.sql.planner.streams.rel.StreamsRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormRelUtils {
    private static final Logger LOG = LoggerFactory.getLogger(StormRelUtils.class);

    private static final AtomicInteger sequence = new AtomicInteger(0);
    private static final AtomicInteger classSequence = new AtomicInteger(0);

    public static String getClassName(StreamsRel relNode) {
        return "Generated_"
                + relNode.getClass().getSimpleName().toUpperCase()
                + "_"
                + relNode.getId()
                + "_"
                + classSequence.getAndIncrement();
    }

    public static StreamsRel getStormRelInput(RelNode input) {
        if (input instanceof RelSubset) {
            // go with known best input
            input = ((RelSubset) input).getBest();
        }
        return (StreamsRel) input;
    }

    public static String explain(final RelNode rel) {
        return explain(rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    }

    public static String explain(final RelNode rel, SqlExplainLevel detailLevel) {
        String explain = "";
        try {
            explain = RelOptUtil.toString(rel);
        } catch (StackOverflowError e) {
            LOG.error("StackOverflowError occurred while extracting plan. Please report it to the dev@ mailing list.");
            LOG.error("RelNode " + rel + " ExplainLevel " + detailLevel, e);
            LOG.error("Forcing plan to empty string and continue... SQL Runner may not working properly after.");
        }
        return explain;
    }
}
