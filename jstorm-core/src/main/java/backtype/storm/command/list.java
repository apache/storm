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

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Activate topology
 * 
 * @author longda
 * 
 */
public class list {
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        
        NimbusClient client = null;
        try {
            
            Map conf = Utils.readStormConfig();
            client = NimbusClient.getConfiguredClient(conf);
            
            if (args.length > 0 && StringUtils.isBlank(args[0]) == false) {
                String topologyName = args[0];
                TopologyInfo info = client.getClient().getTopologyInfoByName(topologyName);
                
                System.out.println("Successfully get topology info \n" + Utils.toPrettyJsonString(info));
            } else {
                ClusterSummary clusterSummary = client.getClient().getClusterInfo();
                
                System.out.println("Successfully get cluster info \n" + Utils.toPrettyJsonString(clusterSummary));
            }
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
    
}
