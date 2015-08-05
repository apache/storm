/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.command;

import java.security.InvalidParameterException;
import java.util.Map;

import backtype.storm.generated.RebalanceOptions;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Active topology
 * 
 * @author longda
 * 
 */
public class rebalance {
    static final String REASSIGN_FLAG = "-r";
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        if (args == null || args.length == 0) {
            printErrorInfo();
            return;
        }
        
        int argsIndex = 0;
        String topologyName = null;
        
        try {
            RebalanceOptions options = new RebalanceOptions();
            options.set_reassign(false);
            options.set_conf(null);
            
            if (args[argsIndex].equalsIgnoreCase(REASSIGN_FLAG)) {
                options.set_reassign(true);
                argsIndex++;
                if (args.length <= argsIndex) {
                    // Topology name is not set.
                    printErrorInfo();
                    return;
                } else {
                    topologyName = args[argsIndex];
                }
            } else {
                topologyName = args[argsIndex];
            }
            
            argsIndex++;
            if (args.length > argsIndex) {
                for (int i = argsIndex; i < args.length; i++) {
                    String arg = args[i];
                    if (arg.endsWith("yaml") || arg.endsWith("prop")) {
                        Map userConf = Utils.loadConf(arg);
                        String jsonConf = Utils.to_json(userConf);
                        options.set_conf(jsonConf);
                    } else {
                        try {
                            int delaySeconds = Integer.parseInt(args[1]);
                            options.set_wait_secs(delaySeconds);
                        } catch (NumberFormatException e) {
                            System.out.println("Unsupported argument found, arg=" + arg + ". Full args are " + args);
                            printErrorInfo();
                            return;
                        }
                    }
                }
            }
            
            submitRebalance(topologyName, options);
            
            System.out.println("Successfully submit command rebalance " + topologyName + ", delaySecs=" + options.get_wait_secs() + ", reassignFlag=" + options.is_reassign() + ", newConfiguration=" + options.get_conf());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    
    private static void printErrorInfo() {
        System.out.println("Error: Invalid parameters!");
        System.out.println("USAGE: jstorm rebalance [-r] TopologyName [DelayTime] [NewConfig]");
    }
    
    public static void submitRebalance(String topologyName, RebalanceOptions options) throws Exception {
        submitRebalance(topologyName, options, null);
    }
    
    public static void submitRebalance(String topologyName, RebalanceOptions options, Map conf) throws Exception {
        Map stormConf = Utils.readStormConfig();
        if (conf != null) {
            stormConf.putAll(conf);
        }
        
        NimbusClient client = null;
        try {
            client = NimbusClient.getConfiguredClient(stormConf);
            client.getClient().rebalance(topologyName, options);
        } catch (Exception e) {
            throw e;
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
    
}
