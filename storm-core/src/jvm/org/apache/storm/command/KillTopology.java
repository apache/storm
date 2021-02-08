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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KillTopology {
    private static final Logger LOG = LoggerFactory.getLogger(KillTopology.class);
    private static int errorCount;

    public static void main(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("w", "wait", null, CLI.AS_INT)
                                    .boolOpt("i", "ignore-errors")
                                    .arg("TOPO", CLI.INTO_LIST)
                                    .parse(args);

        @SuppressWarnings("unchecked")
        final List<String> names = (List<String>) cl.get("TOPO");

        // if '-i' is set, we'll try to kill every topology listed, even if an error occurs
        Boolean continueOnError = (Boolean) cl.get("i");

        errorCount = 0;
        Iterator<String> iterator = names.iterator();
        while (iterator.hasNext()) {
            String name = iterator.next();
            try {
                Utils.validateTopologyName(name);
            } catch (IllegalArgumentException e) {
                if (!continueOnError) {
                    throw e;
                } else {
                    iterator.remove();
                    errorCount += 1;
                    LOG.error("Format of topology name {} is not valid ", name);
                }
            }
        }

        if (names.isEmpty()) {
            throw new RuntimeException("Failed to successfully kill " + errorCount + " topologies.");
        }

        // Wait this many seconds after deactivating topology before killing
        Integer wait = (Integer) cl.get("w");

        final KillOptions opts = new KillOptions();
        if (wait != null) {
            opts.set_wait_secs(wait);
        }

        NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
            @Override
            public void run(Nimbus.Iface nimbus) throws Exception {
                for (String name : names) {
                    try {
                        nimbus.killTopologyWithOpts(name, opts);
                        LOG.info("Killed topology: {}", name);
                    } catch (Exception e) {
                        errorCount += 1;
                        if (!continueOnError) {
                            throw e;
                        } else {
                            LOG.error(
                                    "Caught error killing topology '{}'; continuing as -i was passed.", name, e
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
