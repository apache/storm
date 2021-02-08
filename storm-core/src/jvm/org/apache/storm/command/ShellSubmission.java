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

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.ServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShellSubmission {
    private static final Logger LOG = LoggerFactory.getLogger(ShellSubmission.class);

    public static void main(String[] args) throws Exception {
        if (args.length <= 1) {
            LOG.error("Arguments should be of the form: <path_to_jar> [argument...]");
            System.exit(-1);
        }
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        try (NimbusClient client = NimbusClient.getConfiguredClient(conf)) {
            NimbusSummary ns = client.getClient().getLeader();
            String host = ns.get_host();
            int port = ns.get_port();
            String jarPath = StormSubmitter.submitJar(conf, args[0]);
            String[] newArgs = (String[]) ArrayUtils.addAll(Arrays.copyOfRange(args, 1, args.length),
                    new String[]{host, String.valueOf(port), jarPath});
            ServerUtils.execCommand(newArgs);
        }
    }
}