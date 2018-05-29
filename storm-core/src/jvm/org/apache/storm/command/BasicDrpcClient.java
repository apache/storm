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
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple client for connecting to DRPC.
 */
public class BasicDrpcClient {
    private static final Logger LOG = LoggerFactory.getLogger(BasicDrpcClient.class);

    private static void runAndPrint(DRPCClient drpc, String func, String arg) throws Exception {
        String result = drpc.execute(func, arg);
        System.out.println(String.format("%s \"%s\" => \"%s\"", func, arg, result));
    }

    /**
     * Main entry point for the basic DRPC client.
     * @param args command line arguments to be parsed
     * @throws Exception on errors
     */
    public static void main(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("f", "function", null, CLI.AS_STRING)
                                    .arg("FUNC_AND_ARGS", CLI.INTO_LIST)
                                    .parse(args);
        final List<String> funcAndArgs = (List<String>) cl.get("FUNC_AND_ARGS");
        final String function = (String) cl.get("f");
        Config conf = new Config();
        try (DRPCClient drpc = DRPCClient.getConfiguredClient(conf)) {
            if (function == null) {
                if (funcAndArgs.size() % 2 != 0) {
                    LOG.error("If no -f is supplied arguments need to be in the form [function arg]. This has {} args", funcAndArgs.size());
                    System.exit(-1);
                }
                for (int i = 0; i < funcAndArgs.size(); i += 2) {
                    String func = funcAndArgs.get(i);
                    String arg = funcAndArgs.get(i + 1);
                    runAndPrint(drpc, func, arg);
                }
            } else {
                for (String arg : funcAndArgs) {
                    runAndPrint(drpc, function, arg);
                }
            }
        }
    }
}
