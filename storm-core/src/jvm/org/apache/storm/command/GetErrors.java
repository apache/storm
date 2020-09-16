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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONValue;

public class GetErrors {
    /**
     * Only get errors for a topology.
     * @param args Used to accept the topology name.
     * @throws Exception on errors.
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Topology name must be provided.");
        }

        final String name = args[0];

        NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
            @Override
            public void run(Nimbus.Iface client) throws Exception {
                GetInfoOptions opts = new GetInfoOptions();
                opts.set_num_err_choice(NumErrorsChoice.ONE);
                Map<String, Object> outputMap = new HashMap<>();
                try {
                    TopologyInfo topologyInfo = client.getTopologyInfoByNameWithOpts(name, opts);
                    String topologyName = topologyInfo.get_name();
                    Map<String, List<ErrorInfo>> topologyErrors = topologyInfo.get_errors();
                    outputMap.put("Topology Name", topologyName);
                    outputMap.put("Comp-Errors", getComponentErrors(topologyErrors));
                } catch (NotAliveException notAliveException) {
                    outputMap.put("Failure", "No topologies running with name " + name);
                }
                System.out.println(JSONValue.toJSONString(outputMap));
            }

            private Map<String, String> getComponentErrors(Map<String, List<ErrorInfo>> topologyErrors) {
                Map<String, String> componentErrorMap = new HashMap<>();
                for (Map.Entry<String, List<ErrorInfo>> compNameToCompErrors : topologyErrors.entrySet()) {
                    String compName = compNameToCompErrors.getKey();
                    List<ErrorInfo> compErrors = compNameToCompErrors.getValue();
                    if (compErrors != null && !compErrors.isEmpty()) {
                        ErrorInfo latestError = compErrors.get(0);
                        componentErrorMap.put(compName, latestError.get_error());
                    }
                }

                return componentErrorMap;
            }
        });
    }
}
