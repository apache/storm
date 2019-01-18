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

import com.google.common.base.Joiner;
import java.util.Map;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Heartbeats {
    private static final Logger LOG = LoggerFactory.getLogger(Heartbeats.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Command and path arguments must be provided.");
        }

        String command = args[0];
        String path = args[1];

        Map<String, Object> conf = Utils.readStormConfig();
        IStateStorage cluster = ClusterUtils.mkStateStorage(conf, conf, new ClusterStateContext());

        LOG.info("Command: [{}]", command);

        switch (command) {
            case "list":
                handleListCommand(cluster, path);
                break;

            case "get":
                handleGetCommand(cluster, path);
                break;

            default:
                LOG.info("Usage: heartbeats [list|get] path");
        }

        try {
            cluster.close();
        } catch (Exception e) {
            LOG.info("Caught exception: {} on close.", e);
        }

        // force process to be terminated
        System.exit(0);
    }

    private static void handleListCommand(IStateStorage cluster, String path) {
        String message = Joiner.on("\n").join(cluster.get_worker_hb_children(path, false));
        LOG.info("list {}:\n{}\n", path, message);
    }

    private static void handleGetCommand(IStateStorage cluster, String path) {
        String message;
        byte[] hb = cluster.get_worker_hb(path, false);
        if (hb != null) {
            Map<String, Object> heartbeatMap = StatsUtil.convertZkWorkerHb(Utils.deserialize(hb, ClusterWorkerHeartbeat.class));
            message = JSONValue.toJSONString(heartbeatMap);
        } else {
            message = "No Heartbeats found";
        }
        LOG.info(message);
    }
}
