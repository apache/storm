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
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.utils.NimbusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KillTopology {
    private static final Logger LOG = LoggerFactory.getLogger(KillTopology.class);

    public static void main(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("w", "wait", null, CLI.AS_INT)
                                    .boolOpt("c", "continue-on-error")
                                    .arg("TOPO", CLI.INTO_LIST)
                                    .parse(args);

        @SuppressWarnings("unchecked")
        final List<String> names = (List<String>) cl.get("TOPO");

        // wait seconds for topology to shut down
        Integer wait = (Integer) cl.get("w");

        // if '-c' set, we'll try to kill every topology listed, even if an error occurs
        Boolean continueOnError = (Boolean) cl.get("c");

        final KillOptions opts = new KillOptions();
        if (wait != null) {
            opts.set_wait_secs(wait);
        }

        NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
            @Override
            public void run(Nimbus.Iface nimbus) throws Exception {
                int errorCount = 0;
                for (String name : names) {
                    try {
                        nimbus.killTopologyWithOpts(name, opts);
                        LOG.info("Killed topology: {}", name);
                    } catch (Exception e) {
                        errorCount += 1;
                        if (!continueOnError) {
                            throw e;
                        } else {
                            LOG.info(
                                    "Caught error killing topology '{}'; continuing as -c was passed. Exception: {}",
                                    name,
                                    e.getClass().getName()
                            );
                        }
                    }
                }

                // If we failed to kill any topology, still exit with failure status
                if (errorCount > 0) {
                    throw new RuntimeException("Failed to successfully kill " + errorCount + " topologies.");
                }
            }
        });
    }
}
