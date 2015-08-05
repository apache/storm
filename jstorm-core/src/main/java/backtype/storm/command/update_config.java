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

import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * Update user configuration
 * 
 * @author basti
 * 
 */
public class update_config {
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        if (args == null || args.length < 2) {
            throw new InvalidParameterException(
                    "[USAGE] update_config topologyName config");
        }

        String topologyName = args[0];

        NimbusClient client = null;
        try {
            Map conf = Utils.readStormConfig();
            client = NimbusClient.getConfiguredClient(conf);

            Map loadConf = Utils.loadConf(args[1]);
            String jsonConf = Utils.to_json(loadConf);
            System.out.println("New configuration:\n" + jsonConf);

            client.getClient().updateConf(topologyName, jsonConf);

            System.out.println("Successfully submit command update_conf "
                    + topologyName);
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
